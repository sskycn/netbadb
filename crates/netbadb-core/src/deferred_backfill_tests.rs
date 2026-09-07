use super::*;
use crate::schema_composition::SchemaCompositionState;
use netbadb_rel::{BinaryOp, Expr, ExprKind};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ExprType, IndexId, SemanticType};
use sha2::{Digest, Sha256};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round50-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn seed(path: &Path) -> Database {
    let mut database = Database::create_catalog(
        path.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            path.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(path.join("coordinator"))),
    )
    .unwrap();
    database
        .execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT, flag BOOLEAN)")
        .unwrap();
    for statement in [
        "INSERT INTO users VALUES (1, 'one', true)",
        "INSERT INTO users VALUES (2, 'two', false)",
        "INSERT INTO users VALUES (3, NULL, NULL)",
    ] {
        database.execute(statement).unwrap();
    }
    database
}

fn adopt(database: &mut Database) -> Transaction {
    let mut transaction = database.begin_transaction().unwrap();
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET legacy = 'updated' WHERE id = 1",
        )
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "INSERT INTO users VALUES (4, 'inserted', true)",
        )
        .unwrap();
    database
        .execute_in(&mut transaction, "DELETE FROM users WHERE id = 2")
        .unwrap();
    database
        .execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceRefining(_)
    ));
    transaction
}

fn affected(result: ExecutionResult) -> u64 {
    let ExecutionResult::AffectedRows(rows) = result else {
        panic!("expected affected rows")
    };
    rows
}

fn assert_physical_index_key(
    database: &mut Database,
    storage: StorageId,
    column: ColumnId,
    key: &ScalarValue,
    expected_ids: &[i64],
) {
    let storage = database.registry.get_mut(storage).unwrap();
    let path = storage
        .access_paths()
        .into_iter()
        .find(|path| path.column_id == column)
        .unwrap()
        .id;
    let view = storage.read_view().unwrap();
    let mut ids = storage
        .point_lookup_columns_with_view(path, key, &[ColumnId(1)], &view)
        .unwrap()
        .into_iter()
        .map(|(_, values)| {
            let ScalarValue::Int64(id) = values[0] else {
                panic!("expected BIGINT id")
            };
            id
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, expected_ids);
}

#[test]
fn real_execute_commit_reopen_and_final_index_populate_one_rewrite() {
    let root = root("full");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    let source = transaction
        .schema_composition
        .adopted_source()
        .unwrap()
        .source_storage;
    let source_index_digest = crate::schema_mutation_journal::heap_rewrite_indexes_digest(
        &database
            .registry
            .get_mut(source)
            .unwrap()
            .heap_rewrite_indexes()
            .unwrap(),
    )
    .unwrap();
    let next_storage = database.next_storage_id().unwrap();

    assert_eq!(
        affected(
            database
                .execute_in(
                    &mut transaction,
                    "UPDATE users SET marker = legacy WHERE legacy IS NOT NULL",
                )
                .unwrap()
        ),
        2
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ));
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .semantic_digest(0)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "1e1bc04b766e0d63bbda95f5d975439f35442d33afc73143e5d68232458216a0"
    );
    assert_eq!(database.next_storage_id().unwrap(), next_storage);
    assert_eq!(
        crate::schema_mutation_journal::heap_rewrite_indexes_digest(
            &database
                .registry
                .get_mut(source)
                .unwrap()
                .heap_rewrite_indexes()
                .unwrap(),
        )
        .unwrap(),
        source_index_digest
    );
    assert_eq!(
        database
            .registry
            .get_mut(source)
            .unwrap()
            .table()
            .columns
            .len(),
        3
    );

    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(_)
        ))
    ));
    let prepared = match database
        .prepare_sql_statement_in(
            &transaction,
            "UPDATE users SET marker = $1 WHERE legacy IS NULL",
            &[Some(PhysicalType::Text)],
        )
        .unwrap()
    {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected UPDATE"),
    };
    assert_eq!(
        affected(
            database
                .execute_prepared_in(
                    &mut transaction,
                    &prepared,
                    &[ScalarValue::Text("missing".into())],
                )
                .unwrap()
        ),
        1
    );
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "CREATE INDEX users_marker_idx ON users(marker)",
        )
        .unwrap();
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceIndexFinalizing(_)
    ));
    assert!(matches!(
        database.execute_in(&mut transaction, "UPDATE users SET marker = 'too late'"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));

    let action_digest = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_digest();
    database.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    assert_eq!(materialized.intent.action_digest, action_digest);
    assert_eq!(materialized.source_copy_passes, 1);
    assert_eq!(materialized.source_rows_copied, 3);
    let crate::schema_mutation_journal::SchemaIndexTablePlan::RewriteHeap { replacement, .. } =
        &materialized.intent.tables[0]
    else {
        panic!("expected one final Heap rewrite")
    };
    let materialized_target = replacement.new_storage();
    assert_eq!(materialized_target, next_storage);
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        database.next_storage_id().unwrap(),
        StorageId(next_storage.0 + 1)
    );
    let target = database.bindings.resolve_single(TableId(2)).unwrap();
    assert_eq!(target, materialized_target);
    let (source_intent, snapshot_digest) = {
        let journal = database.mutation_journal.as_ref().unwrap().borrow();
        (
            journal
                .source_backfill_intents
                .get(&transaction.id())
                .unwrap()
                .clone(),
            journal
                .compositions
                .get(&transaction.id())
                .unwrap()
                .index_intent
                .as_ref()
                .unwrap()
                .snapshot_digest
                .unwrap(),
        )
    };
    let mut clone_plan = Sha256::new();
    clone_plan.update(transaction.id().0.to_le_bytes());
    clone_plan.update(source.0.to_le_bytes());
    clone_plan.update(target.0.to_le_bytes());
    clone_plan.update(action_digest);
    clone_plan.update(snapshot_digest);
    assert_eq!(
        source_intent.clone_plan_digest,
        <[u8; 32]>::from(clone_plan.finalize())
    );
    assert_eq!(database.indexes(TableId(2)).unwrap().len(), 1);
    assert_eq!(
        database
            .query("SELECT id, marker FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("updated".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("missing".into())],
            vec![ScalarValue::Int64(4), ScalarValue::Text("inserted".into())],
        ]
    );
    assert_physical_index_key(
        &mut database,
        target,
        ColumnId(4),
        &ScalarValue::Text("updated".into()),
        &[1],
    );
    assert_physical_index_key(
        &mut database,
        target,
        ColumnId(4),
        &ScalarValue::Text("missing".into()),
        &[3],
    );
    database.close().unwrap();
    database = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        database
            .query("SELECT marker FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Text("updated".into())],
            vec![ScalarValue::Text("missing".into())],
            vec![ScalarValue::Text("inserted".into())],
        ]
    );
    let reopened_target = database.bindings.resolve_single(TableId(2)).unwrap();
    assert_physical_index_key(
        &mut database,
        reopened_target,
        ColumnId(4),
        &ScalarValue::Text("inserted".into()),
        &[4],
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn ineligible_relational_access_falls_through_without_changing_program() {
    let root = root("gate");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    for statement in [
        "UPDATE users SET legacy = 'base write'",
        "SELECT marker FROM users",
        "DELETE FROM users WHERE id = 1",
    ] {
        assert!(matches!(
            database.execute_in(&mut transaction, statement),
            Err(DatabaseError::SchemaMutation(
                SchemaMutationError::MigrationDataAccessAfterRefinement
            ))
        ));
        assert!(
            transaction
                .schema_composition
                .plan()
                .unwrap()
                .deferred_backfill
                .is_empty()
        );
    }
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn first_action_freezes_layout_and_ordered_actions_are_last_wins() {
    let root = root("ordered");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    assert_eq!(
        affected(
            database
                .execute_in(
                    &mut transaction,
                    "UPDATE users SET marker = 'first' WHERE id = 1",
                )
                .unwrap()
        ),
        1
    );
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "ALTER TABLE users ADD COLUMN forbidden TEXT"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaMutationAfterMaterialization
        ))
    ));
    assert_eq!(
        affected(
            database
                .execute_in(
                    &mut transaction,
                    "UPDATE users SET marker = 'second' WHERE id = 1",
                )
                .unwrap()
        ),
        1
    );
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        database
            .query("SELECT marker FROM users WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Text("second".into())]]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepare_is_pure_stale_dependencies_do_not_append_and_bound_values_are_owned() {
    let root = root("prepare");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    let prepared = match database
        .prepare_sql_statement_in(
            &transaction,
            "UPDATE users SET marker = $1 WHERE id = $2",
            &[Some(PhysicalType::Text), Some(PhysicalType::Int64)],
        )
        .unwrap()
    {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected UPDATE"),
    };
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .is_empty()
    );
    assert_eq!(
        affected(
            database
                .execute_prepared_in(
                    &mut transaction,
                    &prepared,
                    &[ScalarValue::Text("owned".into()), ScalarValue::Int64(1)],
                )
                .unwrap()
        ),
        1
    );
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .len(),
        1
    );
    assert_eq!(
        affected(
            database
                .execute_prepared_in(
                    &mut transaction,
                    &prepared,
                    &[
                        ScalarValue::Text("owned-again".into()),
                        ScalarValue::Int64(3)
                    ],
                )
                .unwrap()
        ),
        1
    );
    let program = &transaction
        .schema_composition
        .plan()
        .unwrap()
        .deferred_backfill;
    assert_eq!(program.len(), 2);
    assert_ne!(program.semantic_digest(0), program.semantic_digest(1));
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET marker = 'owned-four' WHERE id = 4",
        )
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();
    assert!(matches!(
        database.execute_prepared_in(
            &mut transaction,
            &prepared,
            &[ScalarValue::Text("stale".into()), ScalarValue::Int64(1)],
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    transaction.rollback().unwrap();
    drop(transaction);

    let mut transaction = adopt(&mut database);
    let stale = match database
        .prepare_sql_statement_in(&transaction, "UPDATE users SET marker = legacy", &[])
        .unwrap()
    {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected UPDATE"),
    };
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users RENAME COLUMN legacy TO contact",
        )
        .unwrap();
    assert!(matches!(
        database.execute_prepared_in(&mut transaction, &stale, &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .is_empty()
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn digest_scenario(name: &str, statements: &[&str]) -> (Vec<[u8; 32]>, [u8; 32]) {
    let root = root(name);
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ADD COLUMN marker_two TEXT",
        )
        .unwrap();
    for statement in statements {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    let program = &transaction
        .schema_composition
        .plan()
        .unwrap()
        .deferred_backfill;
    let digests = (0..program.len())
        .map(|index| program.semantic_digest(index).unwrap())
        .collect();
    let action_digest = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_digest();
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    (digests, action_digest)
}

#[test]
fn canonical_digest_is_sensitive_to_literal_target_predicate_operator_and_action_order() {
    let (base, _) = digest_scenario(
        "digest-base",
        &["UPDATE users SET marker = 'value' WHERE id = 1"],
    );
    let (literal, _) = digest_scenario(
        "digest-literal",
        &["UPDATE users SET marker = 'different' WHERE id = 1"],
    );
    let (target, _) = digest_scenario(
        "digest-target",
        &["UPDATE users SET marker_two = 'value' WHERE id = 1"],
    );
    let (predicate, _) = digest_scenario(
        "digest-predicate",
        &["UPDATE users SET marker = 'value' WHERE id = 3"],
    );
    let (operator, _) = digest_scenario(
        "digest-operator",
        &["UPDATE users SET marker = 'value' WHERE id != 1"],
    );
    assert_ne!(base[0], literal[0]);
    assert_ne!(base[0], target[0]);
    assert_ne!(base[0], predicate[0]);
    assert_ne!(base[0], operator[0]);

    let (_, forward) = digest_scenario(
        "digest-forward",
        &[
            "UPDATE users SET marker = 'first' WHERE id = 1",
            "UPDATE users SET marker = 'second' WHERE id = 3",
        ],
    );
    let (_, reverse) = digest_scenario(
        "digest-reverse",
        &[
            "UPDATE users SET marker = 'second' WHERE id = 3",
            "UPDATE users SET marker = 'first' WHERE id = 1",
        ],
    );
    assert_ne!(forward, reverse);
}

#[test]
fn empty_match_is_accepted_and_action_limit_is_checked_before_another_scan() {
    let root = root("limits");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    for _ in 0..32 {
        assert_eq!(
            affected(
                database
                    .execute_in(
                        &mut transaction,
                        "UPDATE users SET marker = 'none' WHERE id = 999",
                    )
                    .unwrap()
            ),
            0
        );
    }
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "UPDATE users SET marker = 'none' WHERE id = 999"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::CompositionLimitExceeded("deferred backfill actions")
        ))
    ));
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .len(),
        32
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn balanced_predicate(first: usize, last: usize) -> String {
    if first == last {
        return format!("id = {first}");
    }
    let middle = first + (last - first) / 2;
    format!(
        "({} OR {})",
        balanced_predicate(first, middle),
        balanced_predicate(middle + 1, last)
    )
}

fn balanced_boolean_expression(leaves: usize) -> Expr {
    if leaves == 1 {
        return Expr {
            kind: ExprKind::Literal(ScalarValue::Bool(true)),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
    }
    let left = leaves / 2;
    Expr {
        kind: ExprKind::Binary {
            operator: BinaryOp::Or,
            left: Box::new(balanced_boolean_expression(left)),
            right: Box::new(balanced_boolean_expression(leaves - left)),
        },
        expr_type: ExprType {
            data_type: SemanticType::physical(PhysicalType::Bool),
            nullable: false,
        },
    }
}

#[test]
fn assignment_node_and_depth_limits_fail_before_action_acceptance() {
    let case_root = root("assignment-limit");
    let mut database = seed(&case_root);
    let mut transaction = adopt(&mut database);
    for index in 0..32 {
        database
            .execute_in(
                &mut transaction,
                &format!("ALTER TABLE users ADD COLUMN extra_{index} TEXT"),
            )
            .unwrap();
    }
    let mut assignments = vec!["marker = 'x'".to_owned()];
    assignments.extend((0..32).map(|index| format!("extra_{index} = 'x'")));
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            &format!("UPDATE users SET {}", assignments.join(", "))
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::CompositionLimitExceeded("deferred assignments")
        ))
    ));
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .is_empty()
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(case_root).unwrap();

    let case_root = root("depth-limit");
    let mut database = seed(&case_root);
    let mut transaction = adopt(&mut database);
    let deep = format!(
        "UPDATE users SET marker = 'x' WHERE {}id = 1",
        "NOT ".repeat(33)
    );
    assert!(matches!(
        database.execute_in(&mut transaction, &deep),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::CompositionLimitExceeded("deferred expression depth")
        ))
    ));
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .is_empty()
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(case_root).unwrap();

    let case_root = root("node-limit");
    let mut database = seed(&case_root);
    let mut transaction = adopt(&mut database);
    let wide = format!(
        "UPDATE users SET marker = 'x' WHERE {}",
        balanced_predicate(1, 128)
    );
    let wide_result = database.execute_in(&mut transaction, &wide);
    assert!(
        matches!(&wide_result, Err(DatabaseError::Compile(_))),
        "{wide_result:?}"
    );
    assert!(matches!(
        crate::deferred_backfill::validate_expression_limits_for_test(
            &balanced_boolean_expression(129)
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::CompositionLimitExceeded("deferred expression nodes")
        ))
    ));
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .is_empty()
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(case_root).unwrap();
}

#[test]
fn finalization_rejects_an_observation_mismatch() {
    let root = root("mismatch");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET marker = legacy WHERE legacy IS NOT NULL",
        )
        .unwrap();
    transaction
        .schema_composition
        .adopted_source_mut()
        .unwrap()
        .logical
        .deferred_backfill
        .corrupt_expected_digest(0);
    assert!(matches!(
        database.commit_transaction(&mut transaction),
        Err(DatabaseError::SchemaMutation(SchemaMutationError::Corrupt(
            "deferred backfill Execute/finalization mismatch"
        )))
    ));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn empty_table_allows_projected_not_null_and_simultaneous_assignments_keep_sql_order() {
    let root = root("empty-and-simultaneous");
    let mut database = seed(&root);
    let mut transaction = database.begin_transaction().unwrap();
    database
        .execute_in(&mut transaction, "DELETE FROM users")
        .unwrap();
    database
        .execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    assert_eq!(
        database
            .execute_in(
                &mut transaction,
                "UPDATE users SET marker = legacy WHERE legacy IS NOT NULL",
            )
            .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    assert!(
        !database
            .schema()
            .table("users")
            .unwrap()
            .column("marker")
            .unwrap()
            .nullable
    );
    drop(transaction);

    database
        .execute("INSERT INTO users VALUES (9, 'nine', true, 'seed')")
        .unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    database
        .execute_in(&mut transaction, "UPDATE users SET legacy = legacy")
        .unwrap();
    database
        .execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN copied TEXT")
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ADD COLUMN copied_score BIGINT",
        )
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ADD COLUMN copied_flag BOOLEAN",
        )
        .unwrap();
    assert_eq!(
        database
            .execute_in(
                &mut transaction,
                "UPDATE users SET copied = legacy, copied_score = id, copied_flag = flag",
            )
            .unwrap(),
        ExecutionResult::AffectedRows(1)
    );
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        database
            .query("SELECT copied, copied_score, copied_flag FROM users")
            .unwrap()
            .rows,
        vec![vec![
            ScalarValue::Text("nine".into()),
            ScalarValue::Int64(9),
            ScalarValue::Bool(true),
        ]]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn mixed_base_and_late_targets_stay_on_the_existing_closed_gate() {
    let root = root("error-matrix");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "UPDATE users SET legacy = 'base', marker = 'late'",
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .is_empty()
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn surviving_not_null_refinement_remains_available_during_backfilling() {
    let root = root("surviving-not-null");
    let mut database = seed(&root);
    let mut transaction = database.begin_transaction().unwrap();
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET legacy = 'three' WHERE legacy IS NULL",
        )
        .unwrap();
    database
        .execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    database
        .execute_in(&mut transaction, "UPDATE users SET marker = legacy")
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN legacy SET NOT NULL",
        )
        .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    assert!(
        !database
            .schema()
            .table("users")
            .unwrap()
            .column("legacy")
            .unwrap()
            .nullable
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn rollback_matrix_restores_s1_and_retains_only_accepted_identity_burns() {
    for stage in 0..6 {
        let root = root(&format!("rollback-stage-{stage}"));
        let mut database = seed(&root);
        let source = database.bindings.resolve_single(TableId(2)).unwrap();
        let base_generation = database.schema_generation();
        let base_version = database.table_schema_version(TableId(2)).unwrap();
        let next_storage = database.next_storage_id().unwrap();
        let source_index_digest = crate::schema_mutation_journal::heap_rewrite_indexes_digest(
            &database
                .registry
                .get_mut(source)
                .unwrap()
                .heap_rewrite_indexes()
                .unwrap(),
        )
        .unwrap();
        let mut transaction = adopt(&mut database);
        database
            .execute_in(
                &mut transaction,
                "UPDATE users SET marker = 'filled' WHERE id = 1",
            )
            .unwrap();

        if stage == 2 {
            assert!(matches!(
                database.execute_in(
                    &mut transaction,
                    "ALTER TABLE users ALTER COLUMN marker SET NOT NULL"
                ),
                Err(DatabaseError::SchemaMutation(
                    SchemaMutationError::NotNullViolation(_)
                ))
            ));
        } else if stage >= 1 {
            database
                .execute_in(
                    &mut transaction,
                    "UPDATE users SET marker = 'filled' WHERE id != 1",
                )
                .unwrap();
            if stage >= 3 {
                database
                    .execute_in(
                        &mut transaction,
                        "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
                    )
                    .unwrap();
            }
        }
        if stage >= 4 {
            database
                .execute_in(
                    &mut transaction,
                    "CREATE INDEX users_marker_idx ON users(marker)",
                )
                .unwrap();
        }
        let staged_target = if stage == 5 {
            database.finalize_adopted_source(&mut transaction).unwrap();
            let materialized = transaction.schema_composition.materialized_index().unwrap();
            let crate::schema_mutation_journal::SchemaIndexTablePlan::RewriteHeap {
                replacement,
                ..
            } = &materialized.intent.tables[0]
            else {
                panic!("expected rollback target")
            };
            Some(replacement.new_storage())
        } else {
            None
        };

        transaction.rollback().unwrap();
        assert_eq!(database.schema_generation(), base_generation);
        assert_eq!(
            database.table_schema_version(TableId(2)),
            Some(base_version)
        );
        assert_eq!(database.bindings.resolve_single(TableId(2)), Ok(source));
        assert!(
            database
                .schema()
                .table("users")
                .unwrap()
                .column("marker")
                .is_none()
        );
        assert!(database.indexes(TableId(2)).unwrap().is_empty());
        assert_eq!(database.next_column_id(TableId(2)), Some(ColumnId(5)));
        assert_eq!(
            database
                .registry
                .get_mut(source)
                .unwrap()
                .heap_rewrite_indexes()
                .unwrap()
                .next_index_id,
            IndexId(1)
        );
        assert_eq!(
            database
                .mutation_journal
                .as_ref()
                .unwrap()
                .borrow()
                .effective_index(TableId(2), IndexId(1)),
            Some(IndexId(if stage >= 4 { 2 } else { 1 }))
        );
        let rolled_back_index_digest = crate::schema_mutation_journal::heap_rewrite_indexes_digest(
            &database
                .registry
                .get_mut(source)
                .unwrap()
                .heap_rewrite_indexes()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(rolled_back_index_digest, source_index_digest);
        assert_eq!(
            database.next_storage_id(),
            Some(StorageId(next_storage.0 + u64::from(stage == 5)))
        );
        if let Some(target) = staged_target {
            assert!(database.registry.get_mut(target).is_none());
        }
        assert_eq!(
            database
                .query("SELECT id, legacy FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
                vec![ScalarValue::Int64(2), ScalarValue::Text("two".into())],
                vec![ScalarValue::Int64(3), ScalarValue::Null],
            ]
        );
        let mut writer_probe = database.begin_transaction().unwrap();
        database
            .execute_in(
                &mut writer_probe,
                "UPDATE users SET legacy = legacy WHERE id = 999",
            )
            .unwrap();
        writer_probe.rollback().unwrap();
        database.close().unwrap();
        database = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(database.bindings.resolve_single(TableId(2)), Ok(source));
        assert!(
            database
                .schema()
                .table("users")
                .unwrap()
                .column("marker")
                .is_none()
        );
        database.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn round50_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND50_CRASH_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let mut database = Database::open_catalog(root.join("catalog")).unwrap();
    let mut transaction = adopt(&mut database);
    database
        .execute_in(&mut transaction, "UPDATE users SET marker = 'filled'")
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "CREATE INDEX users_marker_idx ON users(marker)",
        )
        .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 50 crash hook was not reached");
}

fn assert_no_stage(path: &Path) {
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        assert!(
            !entry.file_name().to_string_lossy().contains(".stage"),
            "{:?}",
            entry.path()
        );
        if entry.file_type().unwrap().is_dir() {
            assert_no_stage(&entry.path());
        }
    }
}

#[test]
fn crash_matrix_discards_pre_cord_programs_and_recovers_winners_without_replay() {
    let cases = [
        ("deferred-backfill-accepted", false, false, false),
        ("composition-intent-durable", false, false, false),
        ("source-backfill-intent-durable", false, false, false),
        ("source-backfill-stage-intent-durable", false, false, false),
        ("source-backfill-mid-copy", false, false, false),
        ("source-backfill-final-indexes-built", false, false, false),
        ("after-prepare-1", true, false, false),
        ("after-all-prepares", true, false, false),
        ("after-durable-decision", true, true, false),
        ("after-commit-1", true, true, false),
        ("after-commit-1", true, true, true),
        ("after-all-commits", true, true, false),
    ];
    for (point, coordinator, winner, reverse) in cases {
        let root = root(&format!("crash-{point}-{reverse}"));
        let database = seed(&root);
        let source = database.bindings.resolve_single(TableId(2)).unwrap();
        database.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "deferred_backfill_tests::round50_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND50_CRASH_ROOT", &root);
        if reverse {
            command.env("NETBADB_REVERSE_PARTICIPANT_COMMIT", "1");
        }
        if coordinator {
            crate::coordinator_crash::configure_child(&mut command, point, &root, point);
        } else {
            command.env("NETBADB_BACKFILL_CRASH_POINT", point);
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(if coordinator { 87 } else { 90 }),
            "{point} {reverse}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
            if winner {
                let marker = reopened
                    .schema()
                    .table("users")
                    .unwrap()
                    .column("marker")
                    .unwrap();
                assert!(!marker.nullable);
                assert_ne!(
                    reopened.bindings.resolve_single(TableId(2)).unwrap(),
                    source
                );
                assert_eq!(reopened.indexes(TableId(2)).unwrap().len(), 1);
                assert_eq!(
                    reopened
                        .query("SELECT id, marker FROM users ORDER BY id")
                        .unwrap()
                        .rows,
                    vec![
                        vec![ScalarValue::Int64(1), ScalarValue::Text("filled".into())],
                        vec![ScalarValue::Int64(3), ScalarValue::Text("filled".into())],
                        vec![ScalarValue::Int64(4), ScalarValue::Text("filled".into())],
                    ]
                );
            } else {
                assert_eq!(reopened.bindings.resolve_single(TableId(2)), Ok(source));
                assert!(
                    reopened
                        .schema()
                        .table("users")
                        .unwrap()
                        .column("marker")
                        .is_none()
                );
                assert_eq!(
                    reopened
                        .query("SELECT id FROM users ORDER BY id")
                        .unwrap()
                        .rows,
                    vec![
                        vec![ScalarValue::Int64(1)],
                        vec![ScalarValue::Int64(2)],
                        vec![ScalarValue::Int64(3)],
                    ]
                );
            }
            reopened.close().unwrap();
        }
        assert_no_stage(&root);
        std::fs::remove_dir_all(root).unwrap();
    }
}
