use std::cmp::Ordering;
use std::collections::HashSet;

use netbadb_index::{
    BTreeHandle, IndexEntry, IndexEntryKey, IndexError, IndexSpec, InternalNode, InternalSeparator,
    LeafNode, MetaNode, compare_entry_keys, compare_key_to_entry, decode_internal, decode_leaf,
    decode_meta, encode_internal, encode_leaf, encode_meta, ensure_entry_fits,
    merge_internals_if_fits, merge_leaves_if_fits, split_internal, split_leaf,
};
use netbadb_types::{PageId, RowId, ScalarValue};

use crate::{HeapStorage, Page, PageType, StorageError, Transaction};

#[derive(Debug)]
struct PreparedPage {
    page_id: PageId,
    before: Page,
    after: Page,
    new_page: bool,
}

#[derive(Debug)]
struct PathEntry {
    page_id: PageId,
    node: InternalNode,
    child_position: usize,
}

#[derive(Debug)]
enum DeleteNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl DeleteNode {
    fn encoded_len(&self, spec: &IndexSpec) -> Result<usize, IndexError> {
        match self {
            Self::Leaf(node) => Ok(encode_leaf(spec, node)?.len()),
            Self::Internal(node) => Ok(encode_internal(spec, node)?.len()),
        }
    }
}

/// Persistence orchestration for one B+Tree stored in the same database file
/// and WAL domain as its heap pages.
pub struct BTree<'a> {
    storage: &'a mut HeapStorage,
    #[cfg(test)]
    fail_after_logs: Option<usize>,
}

impl<'a> BTree<'a> {
    pub(crate) fn new(storage: &'a mut HeapStorage) -> Self {
        Self {
            storage,
            #[cfg(test)]
            fail_after_logs: None,
        }
    }

    /// Creates and commits an empty tree, returning its stable metadata-page
    /// handle.
    pub fn create(&mut self, spec: IndexSpec) -> Result<BTreeHandle, StorageError> {
        let mut transaction = self.storage.begin_transaction()?;
        match self.create_in(&mut transaction, spec) {
            Ok(handle) => {
                transaction.commit()?;
                Ok(handle)
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback) => Err(rollback),
            },
        }
    }

    /// Creates an empty tree as part of the caller's active transaction.
    pub fn create_in(
        &mut self,
        transaction: &mut Transaction,
        spec: IndexSpec,
    ) -> Result<BTreeHandle, StorageError> {
        self.storage.validate_transaction(transaction)?;
        transaction.acquire_writer()?;
        let capacity = Page::single_payload_capacity();
        let meta_page = PageId(self.storage.buffer().page_count());
        let root_page = PageId(
            meta_page
                .0
                .checked_add(1)
                .ok_or(IndexError::InvalidChild(meta_page))?,
        );
        let meta = MetaNode {
            root_page,
            height: 1,
            spec: spec.clone(),
        };
        let meta_payload = encode_meta(&meta)?;
        if meta_payload.len() > capacity {
            return Err(IndexError::NodeTooLarge {
                size: meta_payload.len(),
                capacity,
            }
            .into());
        }
        let root_payload = encode_leaf(&spec, &LeafNode::empty())?;
        let changes = vec![
            prepare_new_page(meta_page, PageType::BTreeMeta, &meta_payload)?,
            prepare_new_page(root_page, PageType::BTreeLeaf, &root_payload)?,
        ];
        self.apply_changes(transaction, changes)?;
        Ok(BTreeHandle { meta_page })
    }

    /// Inserts and commits one complete `(key, RowId)` entry.
    pub fn insert(
        &mut self,
        handle: BTreeHandle,
        key: ScalarValue,
        row_id: RowId,
    ) -> Result<(), StorageError> {
        let mut transaction = self.storage.begin_transaction()?;
        match self.insert_in(&mut transaction, handle, key, row_id) {
            Ok(()) => {
                transaction.commit()?;
                Ok(())
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback) => Err(rollback),
            },
        }
    }

    /// Inserts one entry as part of the caller's active transaction.
    pub fn insert_in(
        &mut self,
        transaction: &mut Transaction,
        handle: BTreeHandle,
        key: ScalarValue,
        row_id: RowId,
    ) -> Result<(), StorageError> {
        self.storage.validate_transaction(transaction)?;
        let meta = self.read_meta(handle)?;
        let entry = IndexEntry { key, row_id };
        let capacity = Page::single_payload_capacity();
        ensure_entry_fits(&meta.spec, &entry, capacity)?;
        transaction.acquire_writer()?;

        let (leaf_page, mut leaf, mut path) = self.find_leaf_for_entry(&meta, &entry)?;
        leaf.insert(&meta.spec, entry)?;
        let leaf_payload = encode_leaf(&meta.spec, &leaf)?;
        if leaf_payload.len() <= capacity {
            let change =
                self.prepare_existing_page(leaf_page, PageType::BTreeLeaf, &leaf_payload)?;
            return self.apply_changes(transaction, vec![change]);
        }

        let mut next_page_id = PageId(self.storage.buffer().page_count());
        let right_leaf_page = take_page_id(&mut next_page_id)?;
        let (left_leaf, right_leaf, mut promoted) = split_leaf(
            &meta.spec,
            leaf.entries,
            leaf.next_leaf,
            right_leaf_page,
            capacity,
        )?;
        let mut changes = vec![
            prepare_new_page(
                right_leaf_page,
                PageType::BTreeLeaf,
                &encode_leaf(&meta.spec, &right_leaf)?,
            )?,
            self.prepare_existing_page(
                leaf_page,
                PageType::BTreeLeaf,
                &encode_leaf(&meta.spec, &left_leaf)?,
            )?,
        ];
        let mut left_page = leaf_page;
        let mut right_page = right_leaf_page;

        while let Some(mut parent) = path.pop() {
            parent
                .node
                .insert_separator(parent.child_position, promoted, right_page)?;
            let payload = encode_internal(&meta.spec, &parent.node)?;
            if payload.len() <= capacity {
                changes.push(self.prepare_existing_page(
                    parent.page_id,
                    PageType::BTreeInternal,
                    &payload,
                )?);
                return self.apply_changes(transaction, changes);
            }

            let parent_right_page = take_page_id(&mut next_page_id)?;
            let (left_parent, next_promoted, right_parent) =
                split_internal(&meta.spec, parent.node, capacity)?;
            changes.push(prepare_new_page(
                parent_right_page,
                PageType::BTreeInternal,
                &encode_internal(&meta.spec, &right_parent)?,
            )?);
            changes.push(self.prepare_existing_page(
                parent.page_id,
                PageType::BTreeInternal,
                &encode_internal(&meta.spec, &left_parent)?,
            )?);
            promoted = next_promoted;
            left_page = parent.page_id;
            right_page = parent_right_page;
        }

        let new_root_page = take_page_id(&mut next_page_id)?;
        let new_root = InternalNode {
            first_child: left_page,
            separators: vec![InternalSeparator {
                key: promoted,
                right_child: right_page,
            }],
        };
        changes.push(prepare_new_page(
            new_root_page,
            PageType::BTreeInternal,
            &encode_internal(&meta.spec, &new_root)?,
        )?);
        let new_height = meta
            .height
            .checked_add(1)
            .ok_or(IndexError::InvalidHeight(meta.height))?;
        let updated_meta = MetaNode {
            root_page: new_root_page,
            height: new_height,
            spec: meta.spec.clone(),
        };
        changes.push(self.prepare_existing_page(
            handle.meta_page,
            PageType::BTreeMeta,
            &encode_meta(&updated_meta)?,
        )?);
        self.apply_changes(transaction, changes)
    }

    /// Deletes and commits exactly one `(key, RowId)` entry.
    pub fn delete(
        &mut self,
        handle: BTreeHandle,
        key: ScalarValue,
        row_id: RowId,
    ) -> Result<(), StorageError> {
        let mut transaction = self.storage.begin_transaction()?;
        match self.delete_in(&mut transaction, handle, key, row_id) {
            Ok(()) => {
                transaction.commit()?;
                Ok(())
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback) => Err(rollback),
            },
        }
    }

    /// Deletes exactly one `(key, RowId)` entry in the caller's transaction.
    /// All merge and root-collapse decisions are preflighted before WAL is
    /// changed, so a missing entry leaves the transaction active.
    pub fn delete_in(
        &mut self,
        transaction: &mut Transaction,
        handle: BTreeHandle,
        key: ScalarValue,
        row_id: RowId,
    ) -> Result<(), StorageError> {
        self.storage.validate_transaction(transaction)?;
        let meta = self.read_meta(handle)?;
        let entry = IndexEntry { key, row_id };
        meta.spec.validate_key(&entry.key)?;
        transaction.acquire_writer()?;

        let (leaf_page, mut leaf, mut path) = self.find_leaf_for_entry(&meta, &entry)?;
        leaf.remove(&meta.spec, &entry)?;
        let capacity = Page::single_payload_capacity();
        if meta.height == 1 {
            let payload = encode_leaf(&meta.spec, &leaf)?;
            let change = self.prepare_existing_page(leaf_page, PageType::BTreeLeaf, &payload)?;
            return self.apply_changes(transaction, vec![change]);
        }

        let soft_min = capacity / 2;
        let mut current_page = leaf_page;
        let mut current = DeleteNode::Leaf(leaf);
        let mut changes = Vec::new();

        loop {
            let current_size = current.encoded_len(&meta.spec)?;
            if current_size >= soft_min {
                changes.push(self.prepare_delete_node(current_page, &meta.spec, &current)?);
                return self.apply_changes(transaction, changes);
            }

            let mut parent = path.pop().ok_or(IndexError::InvalidHeight(meta.height))?;
            let merge = self.preflight_merge(
                &meta.spec,
                &parent.node,
                parent.child_position,
                current_page,
                &current,
                capacity,
            )?;
            let Some((retained_page, merged, separator_position)) = merge else {
                changes.push(self.prepare_delete_node(current_page, &meta.spec, &current)?);
                return self.apply_changes(transaction, changes);
            };

            changes.push(self.prepare_delete_node(retained_page, &meta.spec, &merged)?);
            parent.node.remove_separator(separator_position)?;

            if path.is_empty() {
                if parent.node.separators.is_empty() {
                    let height = meta
                        .height
                        .checked_sub(1)
                        .filter(|height| *height >= 1)
                        .ok_or(IndexError::InvalidHeight(meta.height))?;
                    let updated_meta = MetaNode {
                        root_page: retained_page,
                        height,
                        spec: meta.spec.clone(),
                    };
                    changes.push(self.prepare_existing_page(
                        handle.meta_page,
                        PageType::BTreeMeta,
                        &encode_meta(&updated_meta)?,
                    )?);
                } else {
                    changes.push(self.prepare_delete_node(
                        parent.page_id,
                        &meta.spec,
                        &DeleteNode::Internal(parent.node),
                    )?);
                }
                return self.apply_changes(transaction, changes);
            }

            current_page = parent.page_id;
            current = DeleteNode::Internal(parent.node);
        }
    }

    /// Returns every RowId for `key` in the explicit persistent tie-break
    /// order. The index does not validate whether those RowIds are live.
    pub fn lookup(
        &self,
        handle: BTreeHandle,
        key: &ScalarValue,
    ) -> Result<Vec<RowId>, StorageError> {
        let meta = self.read_meta(handle)?;
        meta.spec.validate_key(key)?;
        let mut leaf_page = self.find_leaf_for_key(&meta, key)?;
        let page_count = self.storage.buffer().page_count();
        let mut visited = HashSet::new();
        let mut rows = Vec::new();
        let mut previous_leaf = None;

        loop {
            if !visited.insert(leaf_page) || visited.len() as u64 > page_count {
                return Err(IndexError::LeafChainCycle { page_id: leaf_page }.into());
            }
            let leaf = self.read_leaf(leaf_page, &meta.spec)?;
            if leaf.entries.is_empty() && (meta.height > 1 || leaf.next_leaf.is_some()) {
                return Err(IndexError::EmptyLeaf { page_id: leaf_page }.into());
            }
            if let Some((previous_page, previous_last)) = &previous_leaf {
                let first = leaf
                    .entries
                    .first()
                    .ok_or(IndexError::EmptyLeaf { page_id: leaf_page })?;
                if compare_entry_keys(previous_last, first) != Ordering::Less {
                    return Err(IndexError::LeafChainOrder {
                        left_page: *previous_page,
                        right_page: leaf_page,
                    }
                    .into());
                }
            }
            let start = leaf
                .entries
                .partition_point(|entry| compare_key_to_entry(key, entry) == Ordering::Greater);
            let mut saw_greater = false;
            for entry in &leaf.entries[start..] {
                match compare_key_to_entry(key, entry) {
                    Ordering::Equal => rows.push(entry.row_id),
                    Ordering::Less => {
                        saw_greater = true;
                        break;
                    }
                    Ordering::Greater => {}
                }
            }
            if saw_greater {
                break;
            }
            let should_follow = leaf
                .entries
                .last()
                .is_some_and(|entry| compare_key_to_entry(key, entry) != Ordering::Less);
            match (should_follow, leaf.next_leaf) {
                (true, Some(next)) => {
                    self.validate_child(next)?;
                    let last = leaf
                        .entries
                        .last()
                        .cloned()
                        .ok_or(IndexError::EmptyLeaf { page_id: leaf_page })?;
                    previous_leaf = Some((leaf_page, last));
                    leaf_page = next;
                }
                _ => break,
            }
        }
        Ok(rows)
    }

    /// Reports whether one complete `(key, RowId)` leaf identity exists.
    /// This does not validate whether the referenced heap row is live.
    pub fn contains_exact(
        &self,
        handle: BTreeHandle,
        key: &ScalarValue,
        row_id: RowId,
    ) -> Result<bool, StorageError> {
        let meta = self.read_meta(handle)?;
        let entry = IndexEntryKey {
            key: key.clone(),
            row_id,
        };
        ensure_entry_fits(&meta.spec, &entry, Page::single_payload_capacity())?;
        let (_, leaf, _) = self.find_leaf_for_entry(&meta, &entry)?;
        Ok(leaf.contains_exact(&meta.spec, &entry)?)
    }

    /// Reads the physical, nominal, and nullability identity persisted in the
    /// tree's stable metadata page.
    pub fn spec(&self, handle: BTreeHandle) -> Result<IndexSpec, StorageError> {
        Ok(self.read_meta(handle)?.spec)
    }

    pub(crate) fn read_meta(&self, handle: BTreeHandle) -> Result<MetaNode, StorageError> {
        self.validate_child(handle.meta_page)?;
        let page = self.storage.buffer().read_page(handle.meta_page)?;
        if page.page().header()?.page_type != PageType::BTreeMeta {
            return Err(IndexError::InvalidNodeType.into());
        }
        let meta = decode_meta(page.page().single_payload(PageType::BTreeMeta)?)?;
        let page_count = self.storage.buffer().page_count();
        if u64::from(meta.height) >= page_count {
            return Err(IndexError::InvalidHeight(meta.height).into());
        }
        self.validate_child(meta.root_page)?;
        Ok(meta)
    }

    fn read_leaf(&self, page_id: PageId, spec: &IndexSpec) -> Result<LeafNode, StorageError> {
        self.validate_child(page_id)?;
        let page = self.storage.buffer().read_page(page_id)?;
        if page.page().header()?.page_type != PageType::BTreeLeaf {
            return Err(IndexError::InvalidNodeType.into());
        }
        Ok(decode_leaf(
            spec,
            page.page().single_payload(PageType::BTreeLeaf)?,
        )?)
    }

    fn read_internal(
        &self,
        page_id: PageId,
        spec: &IndexSpec,
    ) -> Result<InternalNode, StorageError> {
        self.validate_child(page_id)?;
        let page = self.storage.buffer().read_page(page_id)?;
        if page.page().header()?.page_type != PageType::BTreeInternal {
            return Err(IndexError::InvalidNodeType.into());
        }
        let node = decode_internal(spec, page.page().single_payload(PageType::BTreeInternal)?)?;
        self.validate_child(node.first_child)?;
        for separator in &node.separators {
            self.validate_child(separator.right_child)?;
        }
        Ok(node)
    }

    fn find_leaf_for_entry(
        &self,
        meta: &MetaNode,
        entry: &IndexEntryKey,
    ) -> Result<(PageId, LeafNode, Vec<PathEntry>), StorageError> {
        let mut page_id = meta.root_page;
        let mut path = Vec::new();
        for level in 1..=meta.height {
            if level == meta.height {
                return Ok((page_id, self.read_leaf(page_id, &meta.spec)?, path));
            }
            let node = self.read_internal(page_id, &meta.spec)?;
            let child_position = node.child_position(entry);
            let child = node.child(child_position)?;
            self.validate_child(child)?;
            path.push(PathEntry {
                page_id,
                node,
                child_position,
            });
            page_id = child;
        }
        Err(IndexError::InvalidHeight(meta.height).into())
    }

    fn find_leaf_for_key(
        &self,
        meta: &MetaNode,
        key: &ScalarValue,
    ) -> Result<PageId, StorageError> {
        let mut page_id = meta.root_page;
        for level in 1..=meta.height {
            if level == meta.height {
                self.read_leaf(page_id, &meta.spec)?;
                return Ok(page_id);
            }
            let node = self.read_internal(page_id, &meta.spec)?;
            let position = node.separators.partition_point(|separator| {
                compare_key_to_entry(key, &separator.key) == Ordering::Greater
            });
            page_id = node.child(position)?;
            self.validate_child(page_id)?;
        }
        Err(IndexError::InvalidHeight(meta.height).into())
    }

    fn validate_child(&self, page_id: PageId) -> Result<(), StorageError> {
        let page_count = self.storage.buffer().page_count();
        if page_id.0 == 0 || page_id.0 >= page_count {
            return Err(IndexError::InvalidChild(page_id).into());
        }
        Ok(())
    }

    fn preflight_merge(
        &self,
        spec: &IndexSpec,
        parent: &InternalNode,
        child_position: usize,
        current_page: PageId,
        current: &DeleteNode,
        capacity: usize,
    ) -> Result<Option<(PageId, DeleteNode, usize)>, StorageError> {
        let (left_page, right_page, separator_position, current_is_left) =
            if child_position < parent.separators.len() {
                (
                    current_page,
                    parent.child(child_position + 1)?,
                    child_position,
                    true,
                )
            } else {
                let separator_position = child_position
                    .checked_sub(1)
                    .ok_or(IndexError::InvalidNodeType)?;
                (
                    parent.child(separator_position)?,
                    current_page,
                    separator_position,
                    false,
                )
            };
        let fence = &parent
            .separators
            .get(separator_position)
            .ok_or(IndexError::InvalidNodeType)?
            .key;

        let merged = match current {
            DeleteNode::Leaf(current_leaf) => {
                let (left, right) = if current_is_left {
                    (current_leaf.clone(), self.read_leaf(right_page, spec)?)
                } else {
                    (self.read_leaf(left_page, spec)?, current_leaf.clone())
                };
                if left.entries.is_empty() && left_page != current_page {
                    return Err(IndexError::EmptyLeaf { page_id: left_page }.into());
                }
                if right.entries.is_empty() && right_page != current_page {
                    return Err(IndexError::EmptyLeaf {
                        page_id: right_page,
                    }
                    .into());
                }
                if left.next_leaf != Some(right_page) {
                    return Err(IndexError::InvalidLeafLink {
                        left_page,
                        expected_right: right_page,
                        actual_right: left.next_leaf,
                    }
                    .into());
                }
                merge_leaves_if_fits(spec, &left, &right, capacity)?.map(DeleteNode::Leaf)
            }
            DeleteNode::Internal(current_internal) => {
                let (left, right) = if current_is_left {
                    (
                        current_internal.clone(),
                        self.read_internal(right_page, spec)?,
                    )
                } else {
                    (
                        self.read_internal(left_page, spec)?,
                        current_internal.clone(),
                    )
                };
                merge_internals_if_fits(spec, &left, fence, &right, capacity)?
                    .map(DeleteNode::Internal)
            }
        };
        Ok(merged.map(|node| (left_page, node, separator_position)))
    }

    fn prepare_delete_node(
        &self,
        page_id: PageId,
        spec: &IndexSpec,
        node: &DeleteNode,
    ) -> Result<PreparedPage, StorageError> {
        match node {
            DeleteNode::Leaf(leaf) => {
                self.prepare_existing_page(page_id, PageType::BTreeLeaf, &encode_leaf(spec, leaf)?)
            }
            DeleteNode::Internal(internal) => self.prepare_existing_page(
                page_id,
                PageType::BTreeInternal,
                &encode_internal(spec, internal)?,
            ),
        }
    }

    fn prepare_existing_page(
        &self,
        page_id: PageId,
        page_type: PageType,
        payload: &[u8],
    ) -> Result<PreparedPage, StorageError> {
        let page = self.storage.buffer().read_page(page_id)?;
        if page.page().header()?.page_type != page_type {
            return Err(IndexError::InvalidNodeType.into());
        }
        let before = page.page().clone();
        drop(page);
        let mut after = before.clone();
        after.replace_single_payload(page_type, payload)?;
        Ok(PreparedPage {
            page_id,
            before,
            after,
            new_page: false,
        })
    }

    fn apply_changes(
        &mut self,
        transaction: &mut Transaction,
        mut changes: Vec<PreparedPage>,
    ) -> Result<(), StorageError> {
        let has_new_pages = changes.iter().any(|change| change.new_page);
        let mut logged = 0_usize;
        let mut last_lsn = None;
        for change in &mut changes {
            #[cfg(test)]
            if self.fail_after_logs == Some(logged) {
                self.fail_after_logs = None;
                transaction.inject_partial_append_failure(0);
            }
            match transaction.log_page_update(&change.before, &mut change.after) {
                Ok(lsn) => {
                    logged += 1;
                    last_lsn = Some(lsn);
                    #[cfg(test)]
                    if logged == 1 {
                        crate::crash_test::maybe_crash(
                            crate::crash_test::TestCrashPoint::BTreeAfterFirstPageUpdateLog,
                        );
                    }
                }
                Err(error) => {
                    if logged != 0 {
                        transaction.require_rollback();
                    }
                    return Err(error);
                }
            }
        }
        if has_new_pages {
            let lsn = last_lsn.ok_or(IndexError::InvalidNodeType)?;
            if let Err(error) = transaction.flush_through(lsn) {
                transaction.require_rollback();
                return Err(error);
            }
        }

        #[cfg(test)]
        let mut published = 0_usize;
        for change in changes.iter().filter(|change| change.new_page) {
            let mut page = match self.storage.buffer().new_page() {
                Ok(page) => page,
                Err(error) => {
                    transaction.require_rollback();
                    return Err(error);
                }
            };
            if page.page_id() != change.page_id {
                transaction.require_rollback();
                return Err(IndexError::InvalidChild(page.page_id()).into());
            }
            *page.page_mut() = change.after.clone();
            drop(page);
            #[cfg(test)]
            {
                published += 1;
                if published == 1 {
                    crate::crash_test::maybe_crash(
                        crate::crash_test::TestCrashPoint::BTreeAfterFirstPagePublish,
                    );
                }
            }
        }
        for change in changes.iter().filter(|change| !change.new_page) {
            let mut page = match self.storage.buffer().write_page(change.page_id) {
                Ok(page) => page,
                Err(error) => {
                    transaction.require_rollback();
                    return Err(error);
                }
            };
            *page.page_mut() = change.after.clone();
            #[cfg(test)]
            {
                drop(page);
                published += 1;
                if published == 1 {
                    crate::crash_test::maybe_crash(
                        crate::crash_test::TestCrashPoint::BTreeAfterFirstPagePublish,
                    );
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn inject_log_failure_after(&mut self, completed_logs: usize) {
        self.fail_after_logs = Some(completed_logs);
    }
}

fn prepare_new_page(
    page_id: PageId,
    page_type: PageType,
    payload: &[u8],
) -> Result<PreparedPage, StorageError> {
    let before = Page::zero(page_id);
    let mut after = Page::new(page_id, page_type);
    after.initialize_single_payload(page_type, payload)?;
    Ok(PreparedPage {
        page_id,
        before,
        after,
        new_page: true,
    })
}

fn take_page_id(next: &mut PageId) -> Result<PageId, IndexError> {
    let current = *next;
    next.0 = next
        .0
        .checked_add(1)
        .ok_or(IndexError::InvalidChild(current))?;
    Ok(current)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use netbadb_index::{
        BTreeHandle, IndexEntry, IndexError, IndexSpec, LeafNode, encode_internal, encode_leaf,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{
        ColumnId, PageId, PhysicalType, RowId, ScalarValue, SemanticType, TableId,
    };

    use crate::{
        HeapStorage, PageType, StorageError, TransactionState, crash_test, wal_alternate_path,
        wal_path,
    };

    use crate::crash_test::TestCrashPoint;

    const PROCESS_CRASH_CHILD_TEST: &str = "btree::tests::process_crash_child_entrypoint";

    fn table() -> TableDef {
        TableDef::new(
            TableId(1),
            "rows",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::UInt64),
            )],
        )
    }

    fn path(case: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("netbadb-btree-{case}-{}", std::process::id()))
    }

    fn cleanup(path: &std::path::Path) {
        let wal = wal_path(path);
        let _ = std::fs::remove_file(wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(path);
    }

    fn row_id(value: u64) -> RowId {
        RowId {
            page: PageId(value / 64 + 1),
            slot: (value % 64) as u16,
            generation: (value % 17 + 1) as u32,
        }
    }

    fn text_spec() -> IndexSpec {
        IndexSpec {
            data_type: SemanticType::physical(PhysicalType::Text),
            nullable: false,
        }
    }

    fn split_key(ordinal: usize) -> ScalarValue {
        ScalarValue::Text(format!("{ordinal:04}-{}", "x".repeat(180)))
    }

    fn split_trigger_ordinal() -> usize {
        let spec = text_spec();
        let mut leaf = LeafNode::empty();
        for ordinal in 0..1_000_usize {
            leaf.insert(
                &spec,
                IndexEntry {
                    key: split_key(ordinal),
                    row_id: row_id(ordinal as u64),
                },
            )
            .expect("build split boundary");
            if encode_leaf(&spec, &leaf)
                .expect("encode split boundary")
                .len()
                > crate::Page::single_payload_capacity()
            {
                return ordinal;
            }
        }
        panic!("fixed-width test keys did not reach a leaf split")
    }

    fn prepare_split_baseline(case: &str) -> (std::path::PathBuf, BTreeHandle, usize) {
        let path = path(case);
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let handle = storage.btree().create(text_spec()).expect("create tree");
        let trigger = split_trigger_ordinal();
        for ordinal in 0..trigger {
            storage
                .btree()
                .insert(handle, split_key(ordinal), row_id(ordinal as u64))
                .expect("prepare full root leaf");
        }
        assert_eq!(storage.btree().read_meta(handle).expect("meta").height, 1);
        storage.close().expect("close split baseline");
        (path, handle, trigger)
    }

    #[test]
    fn capacity_one_tree_grows_to_arbitrary_height_and_reopens_after_checkpoint() {
        let path = path("height-reopen");
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let handle = storage.btree().create(text_spec()).expect("create tree");
        let mut expected = BTreeMap::<String, Vec<RowId>>::new();
        for ordinal in 0..420_u64 {
            let key = format!("{:04}-{}", (ordinal * 73) % 420, "x".repeat(180));
            let row = row_id(ordinal);
            storage
                .btree()
                .insert(handle, ScalarValue::Text(key.clone()), row)
                .expect("insert entry");
            expected.entry(key).or_default().push(row);
        }
        let meta = storage.btree().read_meta(handle).expect("read meta");
        assert!(meta.height >= 3, "expected an internal split");
        for (key, rows) in &expected {
            assert_eq!(
                storage
                    .btree()
                    .lookup(handle, &ScalarValue::Text(key.clone()))
                    .expect("lookup"),
                *rows
            );
        }
        storage.checkpoint().expect("checkpoint tree");
        storage.close().expect("close tree");

        let mut reopened =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("reopen tree");
        assert_eq!(
            reopened.btree().spec(handle).expect("persisted spec"),
            text_spec()
        );
        for (key, rows) in &expected {
            assert_eq!(
                reopened
                    .btree()
                    .lookup(handle, &ScalarValue::Text(key.clone()))
                    .expect("lookup reopened"),
                *rows
            );
        }
        reopened.close().expect("close reopened tree");
        cleanup(&path);
    }

    #[test]
    fn duplicate_nullable_and_typed_keys_lookup_in_deterministic_row_id_order() {
        let path = path("duplicate-null");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let spec = IndexSpec {
            data_type: SemanticType::named("UserId", PhysicalType::UInt64),
            nullable: true,
        };
        let handle = storage.btree().create(spec.clone()).expect("create tree");
        let mut expected = Vec::new();
        for ordinal in (0..240_u64).rev() {
            let row = row_id(ordinal);
            storage
                .btree()
                .insert(handle, ScalarValue::UInt64(42), row)
                .expect("insert duplicate");
            expected.push(row);
        }
        expected.sort_by(|left, right| netbadb_index::compare_row_ids(*left, *right));
        assert_eq!(
            storage
                .btree()
                .lookup(handle, &ScalarValue::UInt64(42))
                .expect("lookup duplicates"),
            expected
        );
        let null_a = row_id(500);
        let null_b = row_id(501);
        storage
            .btree()
            .insert(handle, ScalarValue::Null, null_b)
            .expect("insert NULL");
        storage
            .btree()
            .insert(handle, ScalarValue::Null, null_a)
            .expect("insert NULL");
        assert_eq!(
            storage
                .btree()
                .lookup(handle, &ScalarValue::Null)
                .expect("lookup NULL"),
            vec![null_a, null_b]
        );
        assert!(
            storage
                .btree()
                .contains_exact(handle, &ScalarValue::UInt64(42), expected[0])
                .expect("contains exact duplicate")
        );
        assert!(
            !storage
                .btree()
                .contains_exact(handle, &ScalarValue::UInt64(42), null_a)
                .expect("reject mismatched exact identity")
        );
        assert!(matches!(
            storage
                .btree()
                .insert(handle, ScalarValue::UInt64(42), expected[0]),
            Err(StorageError::Index(IndexError::DuplicateEntry))
        ));
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn exact_delete_preserves_duplicates_and_not_found_keeps_transaction_active() {
        let path = path("exact-delete");
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let handle = storage
            .btree()
            .create(IndexSpec {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: true,
            })
            .expect("create tree");
        let rows = [row_id(1), row_id(2), row_id(3)];
        for row in rows {
            storage
                .btree()
                .insert(handle, ScalarValue::UInt64(42), row)
                .expect("insert duplicate");
        }
        storage
            .btree()
            .delete(handle, ScalarValue::UInt64(42), rows[1])
            .expect("delete exact duplicate");
        assert_eq!(
            storage
                .btree()
                .lookup(handle, &ScalarValue::UInt64(42))
                .expect("lookup duplicates"),
            vec![rows[0], rows[2]]
        );

        let mut transaction = storage.begin_transaction().expect("begin transaction");
        let missing = storage
            .btree()
            .delete_in(&mut transaction, handle, ScalarValue::UInt64(42), rows[1])
            .expect_err("second exact delete must fail");
        assert!(matches!(
            missing,
            StorageError::Index(IndexError::EntryNotFound)
        ));
        assert_eq!(transaction.state(), TransactionState::Active);
        storage
            .btree()
            .insert_in(&mut transaction, handle, ScalarValue::Null, row_id(10))
            .expect("transaction remains writable");
        transaction.commit().expect("commit after not found");
        storage.close().expect("close tree");

        let mut reopened =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("reopen tree");
        assert_eq!(
            reopened
                .btree()
                .lookup(handle, &ScalarValue::UInt64(42))
                .expect("lookup reopened"),
            vec![rows[0], rows[2]]
        );
        assert_eq!(
            reopened
                .btree()
                .lookup(handle, &ScalarValue::Null)
                .expect("lookup NULL"),
            vec![row_id(10)]
        );
        reopened.close().expect("close reopened tree");
        cleanup(&path);
    }

    #[test]
    fn exact_delete_handles_duplicate_key_entries_across_leaf_boundaries() {
        let path = path("duplicate-delete-across-leaves");
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let handle = storage
            .btree()
            .create(IndexSpec {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            })
            .expect("create tree");
        let mut expected = (0..240_u64).map(row_id).collect::<Vec<_>>();
        for row in expected.iter().rev().copied() {
            storage
                .btree()
                .insert(handle, ScalarValue::UInt64(42), row)
                .expect("insert duplicate");
        }
        assert!(storage.btree().read_meta(handle).expect("meta").height > 1);
        for ordinal in [0_usize, 119, 239] {
            let removed = row_id(ordinal as u64);
            storage
                .btree()
                .delete(handle, ScalarValue::UInt64(42), removed)
                .expect("delete boundary duplicate");
            expected.retain(|row| *row != removed);
            assert_eq!(
                storage
                    .btree()
                    .lookup(handle, &ScalarValue::UInt64(42))
                    .expect("lookup duplicates"),
                expected
            );
        }
        storage.close().expect("close tree");
        cleanup(&path);
    }

    #[test]
    fn delete_validates_signed_text_null_type_and_row_id_before_wal() {
        for (case, spec, key) in [
            (
                "signed-delete",
                IndexSpec {
                    data_type: SemanticType::physical(PhysicalType::Int64),
                    nullable: false,
                },
                ScalarValue::Int64(-42),
            ),
            ("text-delete", text_spec(), ScalarValue::Text("键".into())),
            (
                "null-delete",
                IndexSpec {
                    data_type: SemanticType::physical(PhysicalType::UInt64),
                    nullable: true,
                },
                ScalarValue::Null,
            ),
        ] {
            let path = path(case);
            cleanup(&path);
            let mut storage = HeapStorage::create(&path, table()).expect("create heap");
            let handle = storage.btree().create(spec).expect("create tree");
            let row = row_id(7);
            storage
                .btree()
                .insert(handle, key.clone(), row)
                .expect("insert typed key");
            storage
                .btree()
                .delete(handle, key.clone(), row)
                .expect("delete typed key");
            assert!(
                storage
                    .btree()
                    .lookup(handle, &key)
                    .expect("lookup")
                    .is_empty()
            );
            storage.close().expect("close tree");
            cleanup(&path);
        }

        let path = path("delete-validation");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let handle = storage
            .btree()
            .create(IndexSpec {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            })
            .expect("create tree");
        let mut transaction = storage.begin_transaction().expect("begin transaction");
        for (key, row) in [
            (ScalarValue::Null, row_id(1)),
            (ScalarValue::Int64(1), row_id(1)),
            (
                ScalarValue::UInt64(1),
                RowId {
                    page: PageId(0),
                    slot: 0,
                    generation: 1,
                },
            ),
            (
                ScalarValue::UInt64(1),
                RowId {
                    page: PageId(1),
                    slot: 0,
                    generation: 0,
                },
            ),
        ] {
            assert!(
                storage
                    .btree()
                    .delete_in(&mut transaction, handle, key, row)
                    .is_err()
            );
            assert_eq!(transaction.state(), TransactionState::Active);
        }
        transaction
            .commit()
            .expect("validation errors do not poison transaction");
        storage.close().expect("close tree");
        cleanup(&path);
    }

    #[test]
    fn deterministic_insert_delete_sequence_matches_reference_map() {
        let path = path("delete-reference-map");
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let handle = storage
            .btree()
            .create(IndexSpec {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            })
            .expect("create tree");
        let mut expected = BTreeMap::<u64, Vec<RowId>>::new();

        for ordinal in 0..320_u64 {
            let key = (ordinal * 37) % 23;
            let row = row_id(ordinal);
            storage
                .btree()
                .insert(handle, ScalarValue::UInt64(key), row)
                .expect("insert reference entry");
            expected.entry(key).or_default().push(row);
        }
        for ordinal in (0..320_u64).step_by(3) {
            let key = (ordinal * 37) % 23;
            let row = row_id(ordinal);
            storage
                .btree()
                .delete(handle, ScalarValue::UInt64(key), row)
                .expect("delete reference entry");
            expected
                .get_mut(&key)
                .expect("reference key")
                .retain(|candidate| *candidate != row);
        }
        for ordinal in 320..400_u64 {
            let key = (ordinal * 19) % 23;
            let row = row_id(ordinal);
            storage
                .btree()
                .insert(handle, ScalarValue::UInt64(key), row)
                .expect("reinsert reference entry");
            expected.entry(key).or_default().push(row);
        }
        for rows in expected.values_mut() {
            rows.sort_by(|left, right| netbadb_index::compare_row_ids(*left, *right));
        }
        for key in 0..23_u64 {
            assert_eq!(
                storage
                    .btree()
                    .lookup(handle, &ScalarValue::UInt64(key))
                    .expect("reference lookup"),
                expected.get(&key).cloned().unwrap_or_default()
            );
        }
        storage.close().expect("close tree");
        cleanup(&path);
    }

    #[test]
    fn root_leaf_delete_can_empty_tree_and_rollback_restores_entry() {
        let path = path("root-leaf-delete");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let handle = storage.btree().create(text_spec()).expect("create tree");
        let original_root = storage.btree().read_meta(handle).expect("meta").root_page;
        let row = row_id(1);
        storage
            .btree()
            .insert(handle, ScalarValue::Text("only".into()), row)
            .expect("insert only entry");
        let mut transaction = storage.begin_transaction().expect("begin delete");
        storage
            .btree()
            .delete_in(
                &mut transaction,
                handle,
                ScalarValue::Text("only".into()),
                row,
            )
            .expect("delete only entry");
        assert!(
            storage
                .btree()
                .lookup(handle, &ScalarValue::Text("only".into()))
                .expect("empty lookup")
                .is_empty()
        );
        transaction.rollback().expect("rollback delete");
        assert_eq!(
            storage
                .btree()
                .lookup(handle, &ScalarValue::Text("only".into()))
                .expect("restored lookup"),
            vec![row]
        );
        storage
            .btree()
            .delete(handle, ScalarValue::Text("only".into()), row)
            .expect("commit delete");
        let meta = storage.btree().read_meta(handle).expect("empty meta");
        assert_eq!(meta.height, 1);
        assert_eq!(meta.root_page, original_root);
        assert!(
            storage
                .btree()
                .read_leaf(meta.root_page, &meta.spec)
                .expect("root leaf")
                .entries
                .is_empty()
        );
        storage.close().expect("close tree");
        cleanup(&path);
    }

    #[test]
    fn heavy_delete_merges_collapses_root_and_allows_regrowth() {
        let path = path("delete-collapse-regrow");
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let handle = storage.btree().create(text_spec()).expect("create tree");
        let count = 420_u64;
        for ordinal in 0..count {
            storage
                .btree()
                .insert(handle, split_key(ordinal as usize), row_id(ordinal))
                .expect("insert tall tree");
        }
        let old_page_count = storage.buffer().page_count();
        assert!(storage.btree().read_meta(handle).expect("tall meta").height >= 3);

        for ordinal in 0..count {
            storage
                .btree()
                .delete(handle, split_key(ordinal as usize), row_id(ordinal))
                .expect("delete tall tree");
            if ordinal % 37 == 0 && ordinal + 1 < count {
                assert_eq!(
                    storage
                        .btree()
                        .lookup(handle, &split_key((ordinal + 1) as usize))
                        .expect("survivor lookup"),
                    vec![row_id(ordinal + 1)]
                );
            }
        }
        let meta = storage.btree().read_meta(handle).expect("collapsed meta");
        assert_eq!(meta.height, 1);
        assert!(
            storage
                .btree()
                .read_leaf(meta.root_page, &meta.spec)
                .expect("empty root")
                .entries
                .is_empty()
        );
        assert_eq!(storage.buffer().page_count(), old_page_count);

        storage
            .btree()
            .insert(handle, ScalarValue::Text("regrown".into()), row_id(999))
            .expect("insert after collapse");
        assert_eq!(
            storage
                .btree()
                .lookup(handle, &ScalarValue::Text("regrown".into()))
                .expect("lookup regrown"),
            vec![row_id(999)]
        );
        storage.checkpoint().expect("checkpoint");
        storage.close().expect("close tree");
        let mut reopened =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("reopen tree");
        assert_eq!(
            reopened
                .btree()
                .lookup(handle, &ScalarValue::Text("regrown".into()))
                .expect("lookup reopened"),
            vec![row_id(999)]
        );
        reopened.close().expect("close reopened tree");
        cleanup(&path);
    }

    #[test]
    fn deleting_live_separator_entry_keeps_persistent_fence_for_future_routing() {
        let (path, handle, trigger) = prepare_split_baseline("persistent-fence");
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("open tree");
        storage
            .btree()
            .insert(handle, split_key(trigger), row_id(trigger as u64))
            .expect("split root");
        for ordinal in trigger + 1..trigger + 8 {
            storage
                .btree()
                .insert(handle, split_key(ordinal), row_id(ordinal as u64))
                .expect("fill right leaf");
        }

        let (meta, root_before, fence) = {
            let tree = storage.btree();
            let meta = tree.read_meta(handle).expect("meta");
            let root = tree
                .read_internal(meta.root_page, &meta.spec)
                .expect("root");
            let fence = root.separators[0].key.clone();
            (meta, root, fence)
        };
        storage
            .btree()
            .delete(handle, fence.key.clone(), fence.row_id)
            .expect("delete live fence entry");
        let root_after = storage
            .btree()
            .read_internal(meta.root_page, &meta.spec)
            .expect("root after delete");
        assert_eq!(root_after, root_before, "delete must not rewrite the fence");
        assert!(
            storage
                .btree()
                .lookup(handle, &fence.key)
                .expect("deleted fence lookup")
                .iter()
                .all(|row| *row != fence.row_id)
        );

        let ScalarValue::Text(old_fence) = &fence.key else {
            panic!("text split must produce a text fence");
        };
        let between = ScalarValue::Text(format!("{old_fence}\0"));
        let between_row = row_id(10_000);
        storage
            .btree()
            .insert(handle, between.clone(), between_row)
            .expect("insert between stale fence and live minimum");
        assert_eq!(
            storage
                .btree()
                .lookup(handle, &between)
                .expect("lookup between"),
            vec![between_row]
        );
        storage.close().expect("close tree");
        cleanup(&path);
    }

    #[test]
    fn merge_log_failure_requires_rollback_and_restores_root() {
        let (path, handle, trigger) = prepare_split_baseline("delete-log-failure");
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("open tree");
        storage
            .btree()
            .insert(handle, split_key(trigger), row_id(trigger as u64))
            .expect("split root");
        let before = storage.btree().read_meta(handle).expect("meta before");
        assert_eq!(before.height, 2);
        let page_count = storage.buffer().page_count();

        let mut transaction = storage.begin_transaction().expect("begin delete");
        let error = {
            let mut tree = storage.btree();
            tree.inject_log_failure_after(1);
            tree.delete_in(
                &mut transaction,
                handle,
                split_key(trigger),
                row_id(trigger as u64),
            )
            .expect_err("second delete PageUpdate must fail")
        };
        assert!(matches!(error, StorageError::Wal(_)));
        assert_eq!(transaction.state(), TransactionState::RollbackRequired);
        assert!(transaction.commit().is_err());
        transaction.rollback().expect("rollback partial merge");
        assert_eq!(storage.buffer().page_count(), page_count);
        assert_eq!(
            storage.btree().read_meta(handle).expect("restored meta"),
            before
        );
        assert_eq!(
            storage
                .btree()
                .lookup(handle, &split_key(trigger))
                .expect("restored entry"),
            vec![row_id(trigger as u64)]
        );
        storage.close().expect("close tree");
        cleanup(&path);
    }

    #[test]
    fn invalid_handle_and_key_fail_before_mutating_tree() {
        let path = path("typed-errors");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let handle = storage
            .btree()
            .create(IndexSpec {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            })
            .expect("create tree");
        assert!(matches!(
            storage.btree().lookup(
                BTreeHandle {
                    meta_page: PageId(0)
                },
                &ScalarValue::UInt64(1)
            ),
            Err(StorageError::Index(IndexError::InvalidChild(PageId(0))))
        ));
        assert!(matches!(
            storage.btree().insert(handle, ScalarValue::Null, row_id(1)),
            Err(StorageError::Index(IndexError::NullNotAllowed))
        ));
        assert!(matches!(
            storage
                .btree()
                .insert(handle, ScalarValue::Int64(1), row_id(1)),
            Err(StorageError::Index(IndexError::TypeMismatch { .. }))
        ));
        assert!(
            storage
                .btree()
                .lookup(handle, &ScalarValue::UInt64(1))
                .expect("empty lookup")
                .is_empty()
        );
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn create_partial_log_failure_requires_rollback_and_restores_page_count() {
        let path = path("create-partial-log-failure");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let baseline_pages = storage.buffer().page_count();
        let mut transaction = storage.begin_transaction().expect("begin create");
        let error = {
            let mut tree = storage.btree();
            tree.inject_log_failure_after(1);
            tree.create_in(&mut transaction, text_spec())
                .expect_err("second create log must fail")
        };
        assert!(matches!(error, StorageError::Wal(_)));
        assert_eq!(transaction.state(), TransactionState::RollbackRequired);
        assert!(
            transaction.commit().is_err(),
            "partial create must not commit"
        );
        transaction.rollback().expect("rollback partial create");
        assert_eq!(storage.buffer().page_count(), baseline_pages);
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn split_partial_log_allocation_and_publication_failures_roll_back_exactly() {
        for failure in ["later-log", "allocation", "publication"] {
            let (path, handle, trigger) = prepare_split_baseline(failure);
            let mut storage =
                HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("open tree");
            let baseline_pages = storage.buffer().page_count();
            let mut transaction = storage.begin_transaction().expect("begin split");
            if failure == "allocation" {
                storage.buffer().inject_partial_page_allocation_failure(137);
            } else if failure == "publication" {
                // Capacity one makes publishing the second new split page
                // evict and write the first published page.
                storage.buffer().inject_page_write_failure();
            }
            let error = {
                let mut tree = storage.btree();
                if failure == "later-log" {
                    tree.inject_log_failure_after(1);
                }
                tree.insert_in(
                    &mut transaction,
                    handle,
                    split_key(trigger),
                    row_id(trigger as u64),
                )
                .expect_err("split must fail")
            };
            assert!(matches!(error, StorageError::Wal(_) | StorageError::Io(_)));
            assert_eq!(transaction.state(), TransactionState::RollbackRequired);
            assert!(
                transaction.commit().is_err(),
                "failed split must not commit"
            );
            transaction.rollback().expect("rollback failed split");
            assert_eq!(storage.buffer().page_count(), baseline_pages);
            assert!(
                storage
                    .btree()
                    .lookup(handle, &split_key(trigger))
                    .expect("lookup after rollback")
                    .is_empty()
            );
            assert_eq!(storage.btree().read_meta(handle).expect("meta").height, 1);
            assert_eq!(
                storage
                    .btree()
                    .lookup(handle, &split_key(trigger - 1))
                    .expect("baseline lookup after rollback"),
                vec![row_id((trigger - 1) as u64)]
            );
            storage.close().expect("close rolled-back tree");
            cleanup(&path);
        }
    }

    #[test]
    fn heap_and_btree_pages_coexist_and_heap_appends_after_index_pages() {
        let path = path("mixed-pages");
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let mut heap_rows = Vec::new();
        heap_rows.push(
            storage
                .insert(&[ScalarValue::UInt64(0)])
                .expect("first row"),
        );
        let handle = storage
            .btree()
            .create(IndexSpec {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            })
            .expect("create mixed tree");
        assert_eq!(handle.meta_page, PageId(3));

        let mut value = 1_u64;
        while heap_rows.last().expect("heap row").page == PageId(2) {
            heap_rows.push(
                storage
                    .insert(&[ScalarValue::UInt64(value)])
                    .expect("append heap row"),
            );
            value += 1;
        }
        assert_eq!(heap_rows.last().expect("new heap page").page, PageId(5));
        for (ordinal, row) in heap_rows.iter().copied().enumerate() {
            storage
                .btree()
                .insert(handle, ScalarValue::UInt64(ordinal as u64), row)
                .expect("index heap row");
        }
        assert!(
            storage.buffer().page_count() > 5,
            "tree should split after heap page 4"
        );
        assert_eq!(
            storage
                .btree()
                .lookup(handle, &ScalarValue::UInt64(0))
                .expect("lookup mixed tree"),
            vec![heap_rows[0]]
        );
        assert!(matches!(
            storage.read_row(RowId {
                page: handle.meta_page,
                slot: 0,
                generation: 1,
            }),
            Err(StorageError::RowNotFound { .. })
        ));
        storage
            .update(heap_rows[0], &[ScalarValue::UInt64(10_000)])
            .expect("update heap among index pages");
        storage
            .delete(heap_rows[1])
            .expect("delete heap among index pages");
        assert_eq!(
            storage.scan().expect("scan mixed pages").len(),
            heap_rows.len() - 1
        );
        storage.checkpoint().expect("checkpoint mixed file");
        storage.close().expect("close mixed file");

        let mut reopened =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("reopen mixed file");
        assert_eq!(
            reopened.scan().expect("scan reopened mixed").len(),
            heap_rows.len() - 1
        );
        assert_eq!(
            reopened
                .btree()
                .lookup(handle, &ScalarValue::UInt64(0))
                .expect("lookup reopened mixed"),
            vec![heap_rows[0]]
        );
        reopened.close().expect("close reopened mixed file");
        cleanup(&path);
    }

    #[test]
    fn page_checksum_and_node_codec_report_distinct_corruption_layers() {
        let path = path("corruption-layers");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let handle = storage.btree().create(text_spec()).expect("create tree");
        {
            let mut page = storage
                .buffer()
                .write_page(handle.meta_page)
                .expect("write meta");
            page.page_mut().bytes_mut()[100] ^= 1;
        }
        assert!(matches!(
            storage.btree().spec(handle),
            Err(StorageError::Page(
                crate::PageError::ChecksumMismatch { .. }
            ))
        ));
        storage.simulate_crash();
        cleanup(&path);

        let mut storage = HeapStorage::create(&path, table()).expect("recreate heap");
        let handle = storage.btree().create(text_spec()).expect("create tree");
        let mut payload = netbadb_index::encode_meta(
            &storage.btree().read_meta(handle).expect("read valid meta"),
        )
        .expect("encode valid meta");
        payload[20] = 99;
        {
            let mut page = storage
                .buffer()
                .write_page(handle.meta_page)
                .expect("write semantic corruption");
            page.page_mut()
                .replace_single_payload(crate::PageType::BTreeMeta, &payload)
                .expect("replace payload and checksum");
        }
        assert!(matches!(
            storage.btree().spec(handle),
            Err(StorageError::Index(IndexError::InvalidPhysicalType(99)))
        ));
        storage.simulate_crash();
        cleanup(&path);
    }

    #[test]
    fn traversal_rejects_an_unselected_out_of_range_internal_child() {
        let (path, handle, trigger) = prepare_split_baseline("invalid-unselected-child");
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("open tree");
        storage
            .btree()
            .insert(handle, split_key(trigger), row_id(trigger as u64))
            .expect("split root leaf");

        let (meta, mut root) = {
            let tree = storage.btree();
            let meta = tree.read_meta(handle).expect("read metadata");
            let root = tree
                .read_internal(meta.root_page, &meta.spec)
                .expect("read root");
            (meta, root)
        };
        let invalid_child = PageId(storage.buffer().page_count() + 10);
        root.separators
            .last_mut()
            .expect("split root separator")
            .right_child = invalid_child;
        let payload = encode_internal(&meta.spec, &root).expect("encode corrupt root");
        {
            let mut page = storage
                .buffer()
                .write_page(meta.root_page)
                .expect("write corrupt root");
            page.page_mut()
                .replace_single_payload(PageType::BTreeInternal, &payload)
                .expect("replace root payload");
        }

        assert!(matches!(
            storage.btree().lookup(handle, &split_key(0)),
            Err(StorageError::Index(IndexError::InvalidChild(page_id)))
                if page_id == invalid_child
        ));
        storage.simulate_crash();
        cleanup(&path);
    }

    #[test]
    fn lookup_rejects_non_increasing_and_empty_next_leaves() {
        for corruption in ["non-increasing", "empty"] {
            let case = format!("invalid-leaf-chain-{corruption}");
            let (path, handle, trigger) = prepare_split_baseline(&case);
            let mut storage =
                HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("open tree");
            storage
                .btree()
                .insert(handle, split_key(trigger), row_id(trigger as u64))
                .expect("split root leaf");

            let (meta, left_page, right_page, left_last, right_next) = {
                let tree = storage.btree();
                let meta = tree.read_meta(handle).expect("read metadata");
                let root = tree
                    .read_internal(meta.root_page, &meta.spec)
                    .expect("read root");
                let left_page = root.first_child;
                let right_page = root
                    .separators
                    .first()
                    .expect("split root separator")
                    .right_child;
                let left = tree
                    .read_leaf(left_page, &meta.spec)
                    .expect("read left leaf");
                let right = tree
                    .read_leaf(right_page, &meta.spec)
                    .expect("read right leaf");
                let left_last = left.entries.last().cloned().expect("non-empty left leaf");
                (meta, left_page, right_page, left_last, right.next_leaf)
            };
            let corrupt_right = LeafNode {
                entries: if corruption == "empty" {
                    vec![]
                } else {
                    vec![left_last.clone()]
                },
                next_leaf: right_next,
            };
            let payload = encode_leaf(&meta.spec, &corrupt_right).expect("encode corrupt leaf");
            {
                let mut page = storage
                    .buffer()
                    .write_page(right_page)
                    .expect("write corrupt leaf");
                page.page_mut()
                    .replace_single_payload(PageType::BTreeLeaf, &payload)
                    .expect("replace leaf payload");
            }

            let error = storage
                .btree()
                .lookup(handle, &left_last.key)
                .expect_err("corrupt leaf chain must fail");
            if corruption == "empty" {
                assert!(matches!(
                    error,
                    StorageError::Index(IndexError::EmptyLeaf { page_id })
                        if page_id == right_page
                ));
            } else {
                assert!(matches!(
                    error,
                    StorageError::Index(IndexError::LeafChainOrder {
                        left_page: actual_left,
                        right_page: actual_right,
                    }) if actual_left == left_page && actual_right == right_page
                ));
            }
            storage.simulate_crash();
            cleanup(&path);
        }
    }

    fn spawn_crash_child(path: &std::path::Path, case: &str, point: TestCrashPoint) {
        let mut command =
            std::process::Command::new(std::env::current_exe().expect("current test executable"));
        command
            .arg("--exact")
            .arg(PROCESS_CRASH_CHILD_TEST)
            .arg("--nocapture");
        crash_test::configure_child(&mut command, case, path, point);
        let status = command.status().expect("start B+Tree crash child");
        assert_eq!(status.code(), Some(crash_test::EXIT_CODE));
    }

    fn run_crash_child(case: &str, path: &std::path::Path) {
        let trigger = split_trigger_ordinal();
        let handle = BTreeHandle {
            meta_page: PageId(3),
        };
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(path, table(), 1).expect("open crash tree");
        match case {
            "split-boundary" => {
                let mut transaction = storage.begin_transaction().expect("begin split loser");
                storage
                    .btree()
                    .insert_in(
                        &mut transaction,
                        handle,
                        split_key(trigger),
                        row_id(trigger as u64),
                    )
                    .expect("perform split until crash point");
            }
            "split-loser-flush" => {
                let mut transaction = storage.begin_transaction().expect("begin split loser");
                storage
                    .btree()
                    .insert_in(
                        &mut transaction,
                        handle,
                        split_key(trigger),
                        row_id(trigger as u64),
                    )
                    .expect("perform loser split");
                storage.flush().expect("flush loser pages");
                crash_test::maybe_crash(TestCrashPoint::ActiveWriterAfterDurablePageFlush);
            }
            "split-winner" => {
                storage
                    .btree()
                    .insert(handle, split_key(trigger), row_id(trigger as u64))
                    .expect("commit split winner");
                crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
            }
            "delete-boundary" => {
                let mut transaction = storage.begin_transaction().expect("begin delete loser");
                storage
                    .btree()
                    .delete_in(
                        &mut transaction,
                        handle,
                        split_key(trigger),
                        row_id(trigger as u64),
                    )
                    .expect("perform delete until crash point");
            }
            "delete-loser-flush" => {
                let mut transaction = storage.begin_transaction().expect("begin delete loser");
                storage
                    .btree()
                    .delete_in(
                        &mut transaction,
                        handle,
                        split_key(trigger),
                        row_id(trigger as u64),
                    )
                    .expect("perform loser delete");
                storage.flush().expect("flush loser pages");
                crash_test::maybe_crash(TestCrashPoint::ActiveWriterAfterDurablePageFlush);
            }
            "delete-winner" => {
                storage
                    .btree()
                    .delete(handle, split_key(trigger), row_id(trigger as u64))
                    .expect("commit delete winner");
                crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
            }
            other => panic!("unknown B+Tree crash case `{other}`"),
        }
        panic!("B+Tree crash child returned without reaching its crash point")
    }

    #[test]
    fn process_crash_child_entrypoint() {
        if std::env::var_os(crash_test::CHILD_ENV).is_none() {
            return;
        }
        let case = std::env::var(crash_test::CASE_ENV).expect("crash case");
        let path = std::env::var_os(crash_test::DATABASE_PATH_ENV)
            .map(std::path::PathBuf::from)
            .expect("crash database path");
        run_crash_child(&case, &path);
    }

    fn assert_split_reopens(path: &std::path::Path, handle: BTreeHandle, present: bool) {
        let trigger = split_trigger_ordinal();
        for _ in 0..2 {
            let mut storage = HeapStorage::open_with_buffer_pool_size(path, table(), 1)
                .expect("recover split tree");
            let rows = storage
                .btree()
                .lookup(handle, &split_key(trigger))
                .expect("lookup recovered split");
            assert_eq!(
                rows,
                if present {
                    vec![row_id(trigger as u64)]
                } else {
                    vec![]
                }
            );
            assert_eq!(
                storage
                    .btree()
                    .read_meta(handle)
                    .expect("recovered meta")
                    .height,
                if present { 2 } else { 1 }
            );
            storage.close().expect("close recovered split tree");
        }
    }

    #[test]
    fn process_crash_split_loser_is_undone_at_log_publish_and_steal_boundaries() {
        for (case, point) in [
            ("first-log", TestCrashPoint::BTreeAfterFirstPageUpdateLog),
            ("first-publish", TestCrashPoint::BTreeAfterFirstPagePublish),
            ("steal", TestCrashPoint::ActiveWriterAfterDurablePageFlush),
        ] {
            let (path, handle, _) = prepare_split_baseline(case);
            let child_case = if case == "steal" {
                "split-loser-flush"
            } else {
                "split-boundary"
            };
            spawn_crash_child(&path, child_case, point);
            assert_split_reopens(&path, handle, false);
            cleanup(&path);
        }
    }

    #[test]
    fn process_crash_no_force_split_winner_redoes_root_and_meta() {
        let (path, handle, _) = prepare_split_baseline("winner");
        spawn_crash_child(
            &path,
            "split-winner",
            TestCrashPoint::CommittedWithoutDataFlush,
        );
        assert_split_reopens(&path, handle, true);
        cleanup(&path);
    }

    fn prepare_delete_collapse_baseline(case: &str) -> (std::path::PathBuf, BTreeHandle) {
        let (path, handle, trigger) = prepare_split_baseline(case);
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("open baseline");
        storage
            .btree()
            .insert(handle, split_key(trigger), row_id(trigger as u64))
            .expect("split baseline root");
        assert_eq!(storage.btree().read_meta(handle).expect("meta").height, 2);
        storage.close().expect("close collapse baseline");
        (path, handle)
    }

    fn assert_delete_collapse_reopens(path: &std::path::Path, handle: BTreeHandle, deleted: bool) {
        let trigger = split_trigger_ordinal();
        for _ in 0..2 {
            let mut storage = HeapStorage::open_with_buffer_pool_size(path, table(), 1)
                .expect("recover delete tree");
            let rows = storage
                .btree()
                .lookup(handle, &split_key(trigger))
                .expect("lookup recovered delete");
            assert_eq!(
                rows,
                if deleted {
                    vec![]
                } else {
                    vec![row_id(trigger as u64)]
                }
            );
            assert_eq!(
                storage
                    .btree()
                    .read_meta(handle)
                    .expect("recovered meta")
                    .height,
                if deleted { 1 } else { 2 }
            );
            storage.close().expect("close recovered delete tree");
        }
    }

    #[test]
    fn process_crash_root_collapse_loser_undoes_log_publish_and_steal() {
        for (case, child_case, point) in [
            (
                "delete-first-log",
                "delete-boundary",
                TestCrashPoint::BTreeAfterFirstPageUpdateLog,
            ),
            (
                "delete-first-publish",
                "delete-boundary",
                TestCrashPoint::BTreeAfterFirstPagePublish,
            ),
            (
                "delete-steal",
                "delete-loser-flush",
                TestCrashPoint::ActiveWriterAfterDurablePageFlush,
            ),
        ] {
            let (path, handle) = prepare_delete_collapse_baseline(case);
            spawn_crash_child(&path, child_case, point);
            assert_delete_collapse_reopens(&path, handle, false);
            cleanup(&path);
        }
    }

    #[test]
    fn process_crash_no_force_root_collapse_winner_redoes_leaf_and_meta() {
        let (path, handle) = prepare_delete_collapse_baseline("delete-winner");
        spawn_crash_child(
            &path,
            "delete-winner",
            TestCrashPoint::CommittedWithoutDataFlush,
        );
        assert_delete_collapse_reopens(&path, handle, true);
        cleanup(&path);
    }
}
