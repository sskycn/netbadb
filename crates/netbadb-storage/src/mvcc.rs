use netbadb_types::{CommandId, CommitSeq, RowId, TxnId};

use crate::txn_status::{SharedTxnStatus, TxnStatus};
use crate::{StorageError, TxnStatusError};

const TUPLE_MAGIC: &[u8; 4] = b"NBMV";
const TUPLE_VERSION: u16 = 1;
pub(crate) const TUPLE_HEADER_SIZE: usize = 48;
const HAS_XMAX: u16 = 1;
const HAS_CMAX: u16 = 2;
const HAS_NEXT_VERSION: u16 = 4;
const KNOWN_FLAGS: u16 = HAS_XMAX | HAS_CMAX | HAS_NEXT_VERSION;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub visible_csn: CommitSeq,
    pub own_txn: Option<TxnId>,
    pub command_id: CommandId,
}

#[derive(Debug)]
pub struct ReadView {
    snapshot: Snapshot,
    statuses: SharedTxnStatus,
}

impl ReadView {
    pub(crate) fn new(snapshot: Snapshot, statuses: SharedTxnStatus) -> Result<Self, StorageError> {
        statuses.borrow_mut().pin_snapshot(snapshot.visible_csn)?;
        Ok(Self { snapshot, statuses })
    }

    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.snapshot
    }
}

impl Drop for ReadView {
    fn drop(&mut self) {
        self.statuses
            .borrow_mut()
            .unpin_snapshot(self.snapshot.visible_csn);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TupleHeader {
    pub(crate) xmin: TxnId,
    pub(crate) xmax: Option<TxnId>,
    pub(crate) cmin: CommandId,
    pub(crate) cmax: Option<CommandId>,
    pub(crate) next_version: Option<RowId>,
}

impl TupleHeader {
    pub(crate) fn inserted_by(xmin: TxnId, cmin: CommandId) -> Self {
        Self {
            xmin,
            xmax: None,
            cmin,
            cmax: None,
            next_version: None,
        }
    }

    pub(crate) fn expire(&mut self, xmax: TxnId, cmax: CommandId, next: Option<RowId>) {
        self.xmax = Some(xmax);
        self.cmax = Some(cmax);
        self.next_version = next;
    }
}

pub(crate) fn encode_tuple(header: &TupleHeader, row_payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0_u8; TUPLE_HEADER_SIZE];
    bytes[0..4].copy_from_slice(TUPLE_MAGIC);
    bytes[4..6].copy_from_slice(&TUPLE_VERSION.to_le_bytes());
    let mut flags = 0_u16;
    if header.xmax.is_some() {
        flags |= HAS_XMAX;
    }
    if header.cmax.is_some() {
        flags |= HAS_CMAX;
    }
    if header.next_version.is_some() {
        flags |= HAS_NEXT_VERSION;
    }
    bytes[6..8].copy_from_slice(&flags.to_le_bytes());
    bytes[8..16].copy_from_slice(&header.xmin.0.to_le_bytes());
    bytes[16..24].copy_from_slice(&header.xmax.map_or(0, |txn| txn.0).to_le_bytes());
    bytes[24..28].copy_from_slice(&header.cmin.0.to_le_bytes());
    bytes[28..32].copy_from_slice(&header.cmax.map_or(0, |command| command.0).to_le_bytes());
    if let Some(next) = header.next_version {
        bytes[32..40].copy_from_slice(&next.page.0.to_le_bytes());
        bytes[40..44].copy_from_slice(&u32::from(next.slot).to_le_bytes());
        bytes[44..48].copy_from_slice(&next.generation.to_le_bytes());
    }
    bytes.extend_from_slice(row_payload);
    bytes
}

pub(crate) fn decode_tuple(payload: &[u8]) -> Result<(TupleHeader, &[u8]), StorageError> {
    if payload.len() < TUPLE_HEADER_SIZE {
        return Err(StorageError::InvalidMvccHeader("tuple header is truncated"));
    }
    if &payload[0..4] != TUPLE_MAGIC {
        return Err(StorageError::InvalidMvccHeader(
            "tuple magic does not match",
        ));
    }
    let version = u16::from_le_bytes([payload[4], payload[5]]);
    if version != TUPLE_VERSION {
        return Err(StorageError::UnsupportedTupleVersion(version));
    }
    let flags = u16::from_le_bytes([payload[6], payload[7]]);
    if flags & !KNOWN_FLAGS != 0 {
        return Err(StorageError::InvalidMvccHeader("tuple flags are invalid"));
    }
    let xmin = TxnId(read_u64(payload, 8)?);
    if xmin.0 == 0 {
        return Err(StorageError::InvalidMvccHeader("tuple xmin is zero"));
    }
    let raw_xmax = read_u64(payload, 16)?;
    let cmin = CommandId(read_u32(payload, 24)?);
    if cmin.0 == 0 {
        return Err(StorageError::InvalidMvccHeader("tuple cmin is zero"));
    }
    let raw_cmax = read_u32(payload, 28)?;
    let raw_next_page = read_u64(payload, 32)?;
    let raw_next_slot = read_u32(payload, 40)?;
    let raw_next_generation = read_u32(payload, 44)?;
    let xmax = optional_txn(flags, raw_xmax)?;
    let cmax = optional_command(flags, raw_cmax)?;
    if xmax.is_some() != cmax.is_some() {
        return Err(StorageError::InvalidMvccHeader(
            "tuple xmax and cmax presence differ",
        ));
    }
    let next_version = if flags & HAS_NEXT_VERSION != 0 {
        let slot = u16::try_from(raw_next_slot)
            .map_err(|_| StorageError::InvalidMvccHeader("version slot exceeds u16"))?;
        if raw_next_page == 0 || raw_next_generation == 0 {
            return Err(StorageError::InvalidMvccHeader(
                "version pointer contains a zero component",
            ));
        }
        Some(RowId {
            page: netbadb_types::PageId(raw_next_page),
            slot,
            generation: raw_next_generation,
        })
    } else {
        if raw_next_page != 0 || raw_next_slot != 0 || raw_next_generation != 0 {
            return Err(StorageError::InvalidMvccHeader(
                "absent version pointer contains data",
            ));
        }
        None
    };
    Ok((
        TupleHeader {
            xmin,
            xmax,
            cmin,
            cmax,
            next_version,
        },
        &payload[TUPLE_HEADER_SIZE..],
    ))
}

fn optional_txn(flags: u16, raw: u64) -> Result<Option<TxnId>, StorageError> {
    if flags & HAS_XMAX != 0 {
        if raw == 0 {
            return Err(StorageError::InvalidMvccHeader("present xmax is zero"));
        }
        Ok(Some(TxnId(raw)))
    } else if raw == 0 {
        Ok(None)
    } else {
        Err(StorageError::InvalidMvccHeader("absent xmax contains data"))
    }
}

fn optional_command(flags: u16, raw: u32) -> Result<Option<CommandId>, StorageError> {
    if flags & HAS_CMAX != 0 {
        if raw == 0 {
            return Err(StorageError::InvalidMvccHeader("present cmax is zero"));
        }
        Ok(Some(CommandId(raw)))
    } else if raw == 0 {
        Ok(None)
    } else {
        Err(StorageError::InvalidMvccHeader("absent cmax contains data"))
    }
}

pub(crate) fn is_visible(header: &TupleHeader, view: &ReadView) -> Result<bool, StorageError> {
    let snapshot = view.snapshot;
    let inserted = if snapshot.own_txn == Some(header.xmin) {
        header.cmin <= snapshot.command_id
    } else {
        match view.statuses.borrow().status(header.xmin)? {
            TxnStatus::Active | TxnStatus::Aborted => false,
            TxnStatus::Committed(sequence) => sequence <= snapshot.visible_csn,
        }
    };
    if !inserted {
        return Ok(false);
    }
    let Some(xmax) = header.xmax else {
        return Ok(true);
    };
    if snapshot.own_txn == Some(xmax) {
        let cmax = header
            .cmax
            .ok_or(StorageError::InvalidMvccHeader("xmax is missing cmax"))?;
        return Ok(cmax > snapshot.command_id);
    }
    match view.statuses.borrow().status(xmax)? {
        TxnStatus::Active | TxnStatus::Aborted => Ok(true),
        TxnStatus::Committed(sequence) => Ok(sequence > snapshot.visible_csn),
    }
}

pub(crate) fn is_dead_before(
    header: &TupleHeader,
    horizon: CommitSeq,
    statuses: &SharedTxnStatus,
) -> Result<bool, TxnStatusError> {
    match statuses.borrow().status(header.xmin)? {
        TxnStatus::Aborted => return Ok(true),
        TxnStatus::Active => return Ok(false),
        TxnStatus::Committed(_) => {}
    }
    let Some(xmax) = header.xmax else {
        return Ok(false);
    };
    match statuses.borrow().status(xmax)? {
        TxnStatus::Committed(sequence) => Ok(sequence <= horizon),
        TxnStatus::Active | TxnStatus::Aborted => Ok(false),
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, StorageError> {
    let end = offset
        .checked_add(8)
        .ok_or(StorageError::InvalidMvccHeader("tuple offset overflow"))?;
    let value = bytes
        .get(offset..end)
        .ok_or(StorageError::InvalidMvccHeader("tuple header is truncated"))?;
    Ok(u64::from_le_bytes(value.try_into().map_err(|_| {
        StorageError::InvalidMvccHeader("tuple u64 is truncated")
    })?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, StorageError> {
    let end = offset
        .checked_add(4)
        .ok_or(StorageError::InvalidMvccHeader("tuple offset overflow"))?;
    let value = bytes
        .get(offset..end)
        .ok_or(StorageError::InvalidMvccHeader("tuple header is truncated"))?;
    Ok(u32::from_le_bytes(value.try_into().map_err(|_| {
        StorageError::InvalidMvccHeader("tuple u32 is truncated")
    })?))
}

#[cfg(test)]
mod tests {
    use super::{TupleHeader, decode_tuple, encode_tuple};
    use crate::StorageError;
    use netbadb_types::{CommandId, PageId, RowId, TxnId};

    #[test]
    fn tuple_header_round_trips_expiration_and_version_pointer() {
        let mut header = TupleHeader::inserted_by(TxnId(7), CommandId(3));
        let next = RowId {
            page: PageId(9),
            slot: 4,
            generation: 2,
        };
        header.expire(TxnId(8), CommandId(5), Some(next));
        let encoded = encode_tuple(&header, b"row");
        let (decoded, row) = decode_tuple(&encoded).expect("decode tuple");
        assert_eq!(decoded, header);
        assert_eq!(row, b"row");
    }

    #[test]
    fn tuple_header_rejects_magic_version_flags_truncation_and_invalid_pointer() {
        let header = TupleHeader::inserted_by(TxnId(1), CommandId(1));
        let encoded = encode_tuple(&header, b"row");
        let mut cases = Vec::new();
        cases.push(encoded[..20].to_vec());
        let mut magic = encoded.clone();
        magic[0] ^= 1;
        cases.push(magic);
        let mut version = encoded.clone();
        version[4..6].copy_from_slice(&2_u16.to_le_bytes());
        cases.push(version);
        let mut flags = encoded.clone();
        flags[6..8].copy_from_slice(&0x8000_u16.to_le_bytes());
        cases.push(flags);
        let mut pointer = encoded;
        pointer[6..8].copy_from_slice(&4_u16.to_le_bytes());
        pointer[32..40].copy_from_slice(&1_u64.to_le_bytes());
        pointer[40..44].copy_from_slice(&u32::from(u16::MAX).saturating_add(1).to_le_bytes());
        pointer[44..48].copy_from_slice(&1_u32.to_le_bytes());
        cases.push(pointer);
        for bytes in cases {
            assert!(matches!(
                decode_tuple(&bytes),
                Err(StorageError::InvalidMvccHeader(_))
                    | Err(StorageError::UnsupportedTupleVersion(_))
            ));
        }
    }
}
