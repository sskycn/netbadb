#[cfg(unix)]
mod operator_admission_tests {
    use super::*;
    use crate::operator::{
        OperatorListenerPolicy, OperatorPhysicalDesignRuntimeToken,
        serve_operator_connection_with_capabilities,
    };
    use crate::{
        OperatorClientError, OperatorErrorCodeV6, OperatorPhysicalColumnarApplyOutcomeV6,
        OperatorPhysicalColumnarDesignModeV6,
        OperatorPhysicalDesignMutationAdmissionDimensionV6 as WireDimension,
        OperatorPhysicalDesignMutationAdmissionRejectionV6 as Rejection,
        OperatorPhysicalIndexApplyOutcomeV6, ServerOperatorClient, ServerOperatorConfig,
    };
    use netbadb_core::{
        PhysicalColumnarMutationPrerequisiteInspection as Prerequisite,
        PhysicalDesignMutationAdmissionConstraint::{AtMost, Unconstrained},
        PhysicalDesignMutationAdmissionDimension as Dimension,
        PhysicalDesignMutationAdmissionLimits, PhysicalDesignMutationConservativeBound as Bound,
    };
    use std::os::unix::net::UnixListener;
    use std::time::Duration;

    #[derive(Clone, Copy)]
    enum Target {
        Index,
        Columnar(PhysicalColumnarDesignMode),
    }
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Created(u64),
        AlreadyApplied(u64),
        AlreadyCovered,
    }

    fn constraint(dimension: Dimension, maximum: u64) -> PhysicalDesignMutationAdmissionPolicy {
        let mut limits = PhysicalDesignMutationAdmissionLimits {
            source_work_units: Unconstrained,
            source_read_bytes: Unconstrained,
            prerequisite_work_units: Unconstrained,
            prerequisite_read_bytes: Unconstrained,
            prerequisite_write_bytes: Unconstrained,
            output_write_bytes: Unconstrained,
        };
        *match dimension {
            Dimension::SourceWorkUnits => &mut limits.source_work_units,
            Dimension::SourceReadBytes => &mut limits.source_read_bytes,
            Dimension::PrerequisiteWorkUnits => &mut limits.prerequisite_work_units,
            Dimension::PrerequisiteReadBytes => &mut limits.prerequisite_read_bytes,
            Dimension::PrerequisiteWriteBytes => &mut limits.prerequisite_write_bytes,
            Dimension::OutputWriteBytes => &mut limits.output_write_bytes,
        } = AtMost(maximum);
        PhysicalDesignMutationAdmissionPolicy::new(limits).unwrap()
    }

    fn configure(
        runtime: &mut ServerPhysicalDesignRuntime,
        target: Target,
        policy: PhysicalDesignMutationAdmissionPolicy,
    ) {
        let mode = ServerOperatorPhysicalDesignMutationAdmission::ComponentLimits(policy);
        match target {
            Target::Index => runtime.operator_admissions.index = mode,
            Target::Columnar(PhysicalColumnarDesignMode::Snapshot) => {
                runtime.operator_admissions.snapshot = mode
            }
            Target::Columnar(PhysicalColumnarDesignMode::Incremental) => {
                runtime.operator_admissions.incremental = mode
            }
        }
    }

    fn runtime(fixture: &mut Fixture, journal: bool) -> ServerPhysicalDesignRuntime {
        let placements = fixture.root.join("placements");
        fs::create_dir_all(&placements).unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            Some(ServerPhysicalColumnarApplyConfig::new(placements, true, true).unwrap()),
            journal.then(|| {
                ServerPhysicalDesignMutationReceiptConfig::new(
                    fixture.root.join("operator.nbmr"),
                    1_000_000,
                )
                .unwrap()
            }),
            &fixture.database,
        )
        .unwrap();
        record_candidate(&mut fixture.database, &mut runtime);
        record_columnar_candidate(&mut fixture.database, &mut runtime);
        runtime
    }

    fn candidate() -> PhysicalColumnarCandidate {
        PhysicalColumnarCandidate {
            table_id: TABLE_ID,
            columns: vec![ColumnId(1)],
        }
    }

    // Actual NBOP codec -> listener -> control -> forwarding -> exactly one worker command.
    fn apply_wire(
        fixture: &mut Fixture,
        runtime: &mut ServerPhysicalDesignRuntime,
        target: Target,
        name: &str,
        token_matches: bool,
        epoch: u64,
    ) -> Result<Outcome, OperatorClientError> {
        let (tx, rx) = mpsc::channel();
        let control = ServerPhysicalDesignControlHandle::new(tx);
        let socket = PathBuf::from(format!(
            "/tmp/netbadb-p35-{}-{}.sock",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let listener = UnixListener::bind(&socket).unwrap();
        let capabilities = runtime.columnar_apply.as_ref().unwrap().capabilities();
        let server = std::thread::spawn(move || {
            let (adaptive_tx, _adaptive_rx) = mpsc::channel();
            let adaptive = crate::ServerAdaptiveControlHandle::new(adaptive_tx);
            let (mut stream, _) = listener.accept().unwrap();
            serve_operator_connection_with_capabilities(
                &mut stream,
                &adaptive,
                &control,
                OperatorListenerPolicy::new(
                    true,
                    true,
                    true,
                    Some(capabilities),
                    Some(OperatorPhysicalDesignRuntimeToken::from_bytes([0x11; 16])),
                ),
            )
            .unwrap();
        });
        let client_config = ServerOperatorConfig::new_with_columnar(
            socket.clone(),
            Duration::from_secs(30),
            true,
            true,
        )
        .unwrap();
        let name = name.to_owned();
        let caller = std::thread::spawn(move || {
            let client = ServerOperatorClient::new(&client_config);
            let token = if token_matches { "11" } else { "22" }.repeat(16);
            match target {
                Target::Index => client
                    .apply_physical_index(token, epoch, TABLE_ID.0, 2, name)
                    .map(|report| match report.outcome {
                        OperatorPhysicalIndexApplyOutcomeV6::Created { index_id } => {
                            Outcome::Created(index_id)
                        }
                        OperatorPhysicalIndexApplyOutcomeV6::AlreadyApplied { index_id } => {
                            Outcome::AlreadyApplied(index_id)
                        }
                        OperatorPhysicalIndexApplyOutcomeV6::AlreadyCovered => {
                            Outcome::AlreadyCovered
                        }
                    }),
                Target::Columnar(mode) => client
                    .apply_physical_columnar(
                        token,
                        epoch,
                        TABLE_ID.0,
                        vec![1],
                        match mode {
                            PhysicalColumnarDesignMode::Snapshot => {
                                OperatorPhysicalColumnarDesignModeV6::Snapshot
                            }
                            PhysicalColumnarDesignMode::Incremental => {
                                OperatorPhysicalColumnarDesignModeV6::Incremental
                            }
                        },
                        name,
                    )
                    .map(|report| match report.outcome {
                        OperatorPhysicalColumnarApplyOutcomeV6::Created { projection_id } => {
                            Outcome::Created(projection_id)
                        }
                        OperatorPhysicalColumnarApplyOutcomeV6::AlreadyApplied {
                            projection_id,
                        } => Outcome::AlreadyApplied(projection_id),
                        OperatorPhysicalColumnarApplyOutcomeV6::AlreadyCovered => {
                            Outcome::AlreadyCovered
                        }
                    }),
            }
        });
        let request = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert!(matches!(
            request,
            ServerPhysicalDesignControlRequest::ApplyApprovedIndex { .. }
                | ServerPhysicalDesignControlRequest::ApplyApprovedColumnar { .. }
        ));
        let (forward_tx, forward_rx) = mpsc::channel();
        forward_tx.send(request).unwrap();
        let mut commands = 0;
        forward_physical_design_control_requests(&forward_rx, |command| {
            commands += 1;
            runtime.handle(&mut fixture.database, command);
            Ok(())
        });
        let result = caller.join().unwrap();
        server.join().unwrap();
        assert_eq!(commands, 1);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
        fs::remove_file(socket).unwrap();
        result
    }

    fn rejection(
        fixture: &mut Fixture,
        runtime: &mut ServerPhysicalDesignRuntime,
        target: Target,
        expected: Rejection,
        journal: bool,
    ) {
        let before = runtime.status();
        let g = current_commit_seq(&fixture.database);
        let error = apply_wire(
            fixture,
            runtime,
            target,
            "approved",
            true,
            before.evidence.epoch.0,
        )
        .unwrap_err();
        let OperatorClientError::Remote(remote) = error else {
            panic!("definite rejection: {error:?}")
        };
        assert_eq!(
            remote.code,
            OperatorErrorCodeV6::PhysicalDesignMutationAdmissionRejected
        );
        let json = serde_json::to_string(&remote).unwrap();
        assert!(!json.contains(fixture.root.to_str().unwrap()));
        assert!(
            !json.contains("Database")
                && !json.contains("Storage")
                && !json.contains("page geometry")
        );
        assert_eq!(remote.admission, Some(expected));
        assert_eq!(remote.receipt.is_some(), journal);
        assert_eq!(runtime.status(), before);
        assert_eq!(current_commit_seq(&fixture.database), g);
        assert!(fixture.database.indexes(TABLE_ID).unwrap().is_empty());
        assert!(fixture.database.inspect_columnar_projections().is_empty());
        assert!(!fixture.root.join("placements/approved").exists());
        if let Some(reference) = remote.receipt {
            let page = receipts(&mut fixture.database, runtime, None, 128).unwrap();
            let last = page.receipts.last().unwrap();
            assert_eq!(last.id.0, reference.receipt_id);
            assert_eq!(
                last.outcome,
                ServerPhysicalDesignMutationReceiptOutcome::Rejected
            );
            let status = receipt_status(&mut fixture.database, runtime).unwrap();
            assert!(!status.recovery_required);
            assert_eq!(
                reference.journal_incarnation,
                status
                    .journal_incarnation
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
        }
    }

    fn discard_recovery_fixture(fixture: Fixture) {
        let Fixture { root, database } = fixture;
        drop(database);
        fs::remove_dir_all(root).unwrap();
    }

    fn assert_storage_recovery_rejection(
        fixture: &mut Fixture,
        runtime: &mut ServerPhysicalDesignRuntime,
        target: Target,
    ) {
        configure(
            runtime,
            target,
            constraint(Dimension::SourceWorkUnits, u64::MAX),
        );
        let before_commit = current_commit_seq(&fixture.database);
        let epoch = runtime.evidence.epoch().0;
        let error = apply_wire(
            fixture,
            runtime,
            target,
            "recovery",
            true,
            epoch,
        )
        .unwrap_err();
        let OperatorClientError::Remote(remote) = error else {
            panic!("recovery inspection must be a definite rejection: {error:?}")
        };
        assert_eq!(
            remote.code,
            OperatorErrorCodeV6::PhysicalDesignMutationAdmissionRejected
        );
        assert_eq!(
            serde_json::to_value(remote.admission.as_ref().unwrap()).unwrap(),
            serde_json::json!({ "kind": "recovery_required" })
        );
        assert_eq!(
            remote.message,
            "current mutation-work inspection requires restart/reopen before retry"
        );
        let reference = remote.receipt.expect("durable Begin is retained");
        assert_eq!(current_commit_seq(&fixture.database), before_commit);
        assert!(fixture.database.indexes(TABLE_ID).unwrap().is_empty());
        assert!(fixture.database.inspect_columnar_projections().is_empty());
        assert!(!fixture.root.join("placements/recovery").exists());
        let page = receipts(&mut fixture.database, runtime, None, 128).unwrap();
        let receipt = page.receipts.last().unwrap();
        assert_eq!(receipt.id.0, reference.receipt_id);
        assert_eq!(
            receipt.outcome,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected
        );
        assert!(!receipt_status(&mut fixture.database, runtime)
            .unwrap()
            .recovery_required);
    }

    #[test]
    fn nbop_existing_projection_catalog_recovery_requires_reopen() {
        let mut fixture = Fixture::create("p35-existing-projection-recovery");
        let mut runtime = runtime(&mut fixture, true);
        fixture
            .database
            .inject_projection_catalog_recovery_required(ColumnarProjectionId(77));
        let before_commit = current_commit_seq(&fixture.database);
        let epoch = runtime.evidence.epoch().0;
        let error = apply_wire(
            &mut fixture,
            &mut runtime,
            Target::Columnar(PhysicalColumnarDesignMode::Snapshot),
            "recovery",
            true,
            epoch,
        )
        .unwrap_err();
        let OperatorClientError::Remote(remote) = error else {
            panic!("existing catalog recovery is definite: {error:?}")
        };
        assert_eq!(
            remote.code,
            OperatorErrorCodeV6::PhysicalColumnarRecoveryRequired
        );
        assert!(remote.admission.is_none());
        assert!(remote.message.contains("restart/reopen"));
        assert!(remote.message.contains("before retrying the exact approval"));
        let reference = remote.receipt.expect("durable Begin is retained");
        assert_eq!(current_commit_seq(&fixture.database), before_commit);
        assert!(fixture.database.inspect_columnar_projections().is_empty());
        assert!(!fixture.root.join("placements/recovery").exists());
        let page = receipts(&mut fixture.database, &mut runtime, None, 128).unwrap();
        let receipt = page.receipts.last().unwrap();
        assert_eq!(receipt.id.0, reference.receipt_id);
        assert_eq!(
            receipt.outcome,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected
        );
        assert!(!receipt_status(&mut fixture.database, &mut runtime)
            .unwrap()
            .recovery_required);
        drop(runtime);
        fixture.close();
    }

    #[test]
    fn operator_index_admission_storage_recovery_requires_reopen() {
        let mut fixture = Fixture::create("p35-index-storage-recovery");
        let mut runtime = runtime(&mut fixture, true);
        fixture
            .database
            .inject_physical_design_storage_recovery_required(TABLE_ID)
            .unwrap();
        assert_storage_recovery_rejection(&mut fixture, &mut runtime, Target::Index);
        drop(runtime);
        discard_recovery_fixture(fixture);
    }

    #[test]
    fn operator_heap_columnar_admission_storage_recovery_requires_reopen() {
        let mut fixture = Fixture::create("p35-heap-columnar-storage-recovery");
        let mut runtime = runtime(&mut fixture, true);
        fixture
            .database
            .inject_physical_design_storage_recovery_required(TABLE_ID)
            .unwrap();
        assert_storage_recovery_rejection(
            &mut fixture,
            &mut runtime,
            Target::Columnar(PhysicalColumnarDesignMode::Snapshot),
        );
        drop(runtime);
        discard_recovery_fixture(fixture);
    }

    #[test]
    fn operator_columnar_admission_storage_recovery_requires_reopen() {
        let mut fixture = lsm_fixture();
        let mut runtime = runtime(&mut fixture, true);
        fixture
            .database
            .inject_physical_design_storage_recovery_required(TABLE_ID)
            .unwrap();
        assert_storage_recovery_rejection(
            &mut fixture,
            &mut runtime,
            Target::Columnar(PhysicalColumnarDesignMode::Snapshot),
        );
        drop(runtime);
        discard_recovery_fixture(fixture);
    }

    #[test]
    fn operator_heap_components_receipts_noops_and_programmatic_authority() {
        for target in [
            Target::Index,
            Target::Columnar(PhysicalColumnarDesignMode::Snapshot),
            Target::Columnar(PhysicalColumnarDesignMode::Incremental),
        ] {
            for journal in [false, true] {
                let mut fixture = Fixture::create("p35-heap");
                fixture.database.enable_change_stream(TABLE_ID).unwrap();
                let mut runtime = runtime(&mut fixture, journal);
                let bounds = match target {
                    Target::Index => {
                        fixture
                            .database
                            .inspect_physical_index_design_mutation_work(CANDIDATE)
                            .unwrap()
                            .bounds
                    }
                    Target::Columnar(mode) => {
                        fixture
                            .database
                            .inspect_physical_columnar_design_mutation_work(&candidate(), mode)
                            .unwrap()
                            .bounds
                    }
                };
                for (dimension, wire, bound) in [
                    (
                        Dimension::SourceWorkUnits,
                        WireDimension::SourceWorkUnits,
                        bounds.source_work_units,
                    ),
                    (
                        Dimension::SourceReadBytes,
                        WireDimension::SourceReadBytes,
                        bounds.source_read_bytes,
                    ),
                ] {
                    let Bound::Bounded(n) = bound else {
                        panic!("Heap proven")
                    };
                    assert!(n > 0);
                    configure(&mut runtime, target, constraint(dimension, n - 1));
                    rejection(
                        &mut fixture,
                        &mut runtime,
                        target,
                        Rejection::LimitExceeded {
                            dimension: wire,
                            conservative_bound: n,
                            maximum: n - 1,
                        },
                        journal,
                    );
                }
                configure(
                    &mut runtime,
                    target,
                    constraint(Dimension::OutputWriteBytes, u64::MAX),
                );
                rejection(
                    &mut fixture,
                    &mut runtime,
                    target,
                    Rejection::RequiredBoundNotProven {
                        dimension: WireDimension::OutputWriteBytes,
                    },
                    journal,
                );
                let Bound::Bounded(work) = bounds.source_work_units else {
                    panic!()
                };
                let Bound::Bounded(bytes) = bounds.source_read_bytes else {
                    panic!()
                };
                let mut limits = constraint(Dimension::SourceWorkUnits, work).limits();
                limits.source_read_bytes = AtMost(bytes);
                limits.prerequisite_work_units = AtMost(0);
                limits.prerequisite_read_bytes = AtMost(0);
                limits.prerequisite_write_bytes = AtMost(0);
                configure(
                    &mut runtime,
                    target,
                    PhysicalDesignMutationAdmissionPolicy::new(limits).unwrap(),
                );
                let epoch = runtime.evidence.epoch().0;
                assert_eq!(
                    apply_wire(&mut fixture, &mut runtime, target, "approved", true, epoch)
                        .unwrap(),
                    Outcome::Created(1)
                );
                // Keep this runtime's policy fixed while legitimate DML grows
                // the source beyond both bounds that admitted the first apply.
                let mut transaction = fixture.database.begin_transaction().unwrap();
                for id in 2..=513 {
                    fixture
                        .database
                        .insert_into_in(
                            TABLE_ID,
                            &mut transaction,
                            &[
                                netbadb_types::ScalarValue::Int64(id),
                                netbadb_types::ScalarValue::Int64(7),
                            ],
                        )
                        .unwrap();
                }
                fixture
                    .database
                    .commit_transaction(&mut transaction)
                    .unwrap();
                let grown = fixture
                    .database
                    .inspect_physical_index_design_mutation_work(CANDIDATE)
                    .unwrap()
                    .bounds;
                assert!(matches!(grown.source_work_units, Bound::Bounded(n) if n > work));
                assert!(matches!(grown.source_read_bytes, Bound::Bounded(n) if n > bytes));
                assert_eq!(
                    apply_wire(&mut fixture, &mut runtime, target, "approved", true, epoch)
                        .unwrap(),
                    Outcome::AlreadyApplied(1)
                );
                configure(
                    &mut runtime,
                    target,
                    constraint(Dimension::OutputWriteBytes, 0),
                );
                assert_eq!(
                    apply_wire(&mut fixture, &mut runtime, target, "approved", false, 999).unwrap(),
                    Outcome::AlreadyApplied(1)
                );
                // Index remains covering after DML. Columnar coverage is tested on a fresh fixture below.
                if matches!(target, Target::Index) {
                    assert_eq!(
                        apply_wire(&mut fixture, &mut runtime, target, "covered", true, epoch)
                            .unwrap(),
                        Outcome::AlreadyCovered
                    );
                }
                drop(runtime);
                fixture.close();
            }
        }
        let mut fixture = Fixture::create("p35-host-independent");
        let mut runtime = runtime(&mut fixture, false);
        configure(
            &mut runtime,
            Target::Index,
            constraint(Dimension::OutputWriteBytes, 0),
        );
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        assert!(matches!(
            apply(&mut fixture.database, &mut runtime, proposal, "host")
                .unwrap()
                .outcome,
            PhysicalIndexDesignApplyOutcome::Created { .. }
        ));
        drop(runtime);
        fixture.close();
    }

    #[test]
    fn operator_inspection_failure_is_private_and_uncertainty_remains_separate() {
        for journal in [false, true] {
            for target in [
                Target::Index,
                Target::Columnar(PhysicalColumnarDesignMode::Snapshot),
            ] {
                let mut fixture = Fixture::create("p35-inspection");
                let mut runtime = runtime(&mut fixture, journal);
                configure(
                    &mut runtime,
                    target,
                    constraint(Dimension::SourceWorkUnits, u64::MAX),
                );
                let file = fs::OpenOptions::new()
                    .write(true)
                    .open(fixture.root.join("events"))
                    .unwrap();
                let len = file.metadata().unwrap().len();
                file.set_len(len + 1).unwrap();
                rejection(
                    &mut fixture,
                    &mut runtime,
                    target,
                    Rejection::InspectionFailed {},
                    journal,
                );
                file.set_len(len).unwrap();
                drop(file);
                runtime.post_apply_failure = Some(TestPostApplyFailure::Database);
                let epoch = runtime.evidence.epoch().0;
                let error = apply_wire(&mut fixture, &mut runtime, target, "approved", true, epoch)
                    .unwrap_err();
                let OperatorClientError::MutationOutcomeUncertain { source, .. } = error else {
                    panic!("uncertainty")
                };
                let OperatorClientError::Remote(remote) = *source else {
                    panic!("remote")
                };
                assert!(remote.admission.is_none());
                assert_eq!(
                    remote.code,
                    OperatorErrorCodeV6::PhysicalDesignMutationOutcomeUncertain
                );
                drop(runtime);
                fixture.close();
            }
        }
    }

    fn lsm_fixture() -> Fixture {
        let root = std::env::temp_dir().join(format!(
            "netbadb-p35-lsm-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let table = TableDef::new(
            TABLE_ID,
            "events",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "category",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
            ],
        );
        let mut database = Database::create_catalog(
            root.join("catalog"),
            vec![TableStorageCreateSpec::lsm(
                root.join("events"),
                table,
                ColumnId(1),
            )],
            Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
        )
        .unwrap();
        database
            .execute("INSERT INTO events VALUES (1, 7)")
            .unwrap();
        Fixture { root, database }
    }

    #[test]
    fn operator_lsm_modes_use_exact_core_bounds_and_partial_component_admission() {
        for mode in [
            PhysicalColumnarDesignMode::Snapshot,
            PhysicalColumnarDesignMode::Incremental,
        ] {
            for empty in [false, true] {
                let mut fixture = lsm_fixture();
                fixture.database.enable_change_stream(TABLE_ID).unwrap();
                fixture.database.flush().unwrap();
                if !empty {
                    fixture
                        .database
                        .execute("INSERT INTO events VALUES (2, 7)")
                        .unwrap();
                }
                let mut runtime = runtime(&mut fixture, true);
                let target = Target::Columnar(mode);
                let before = fixture
                    .database
                    .inspect_lsm_storage(TABLE_ID)
                    .unwrap()
                    .unwrap();
                configure(
                    &mut runtime,
                    target,
                    constraint(Dimension::SourceWorkUnits, u64::MAX),
                );
                rejection(
                    &mut fixture,
                    &mut runtime,
                    target,
                    Rejection::RequiredBoundNotProven {
                        dimension: WireDimension::SourceWorkUnits,
                    },
                    true,
                );
                let inspection = fixture
                    .database
                    .inspect_physical_columnar_design_mutation_work(&candidate(), mode)
                    .unwrap();
                let policy = if mode == PhysicalColumnarDesignMode::Snapshot && !empty {
                    configure(
                        &mut runtime,
                        target,
                        constraint(Dimension::SourceReadBytes, u64::MAX),
                    );
                    rejection(
                        &mut fixture,
                        &mut runtime,
                        target,
                        Rejection::RequiredBoundNotProven {
                            dimension: WireDimension::SourceReadBytes,
                        },
                        true,
                    );
                    let Prerequisite::LsmFlush {
                        conservative_bound: bound,
                        ..
                    } = inspection.prerequisite
                    else {
                        panic!("flush bound")
                    };
                    for (dimension, wire, n) in [
                        (
                            Dimension::PrerequisiteWorkUnits,
                            WireDimension::PrerequisiteWorkUnits,
                            bound.work_units,
                        ),
                        (
                            Dimension::PrerequisiteReadBytes,
                            WireDimension::PrerequisiteReadBytes,
                            bound.read_bytes,
                        ),
                        (
                            Dimension::PrerequisiteWriteBytes,
                            WireDimension::PrerequisiteWriteBytes,
                            bound.write_bytes,
                        ),
                    ] {
                        if n > 0 {
                            configure(&mut runtime, target, constraint(dimension, n - 1));
                            rejection(
                                &mut fixture,
                                &mut runtime,
                                target,
                                Rejection::LimitExceeded {
                                    dimension: wire,
                                    conservative_bound: n,
                                    maximum: n - 1,
                                },
                                true,
                            );
                        }
                    }
                    // Only prerequisite writes are constrained: partial component admission.
                    constraint(Dimension::PrerequisiteWriteBytes, bound.write_bytes)
                } else {
                    let Bound::Bounded(n) = inspection.bounds.source_read_bytes else {
                        panic!("SSTable bytes")
                    };
                    assert!(n > 0);
                    configure(
                        &mut runtime,
                        target,
                        constraint(Dimension::SourceReadBytes, n - 1),
                    );
                    rejection(
                        &mut fixture,
                        &mut runtime,
                        target,
                        Rejection::LimitExceeded {
                            dimension: WireDimension::SourceReadBytes,
                            conservative_bound: n,
                            maximum: n - 1,
                        },
                        true,
                    );
                    let mut limits = constraint(Dimension::SourceReadBytes, n).limits();
                    limits.prerequisite_work_units = AtMost(0);
                    limits.prerequisite_read_bytes = AtMost(0);
                    limits.prerequisite_write_bytes = AtMost(0);
                    PhysicalDesignMutationAdmissionPolicy::new(limits).unwrap()
                };
                assert_eq!(
                    fixture
                        .database
                        .inspect_lsm_storage(TABLE_ID)
                        .unwrap()
                        .unwrap(),
                    before
                );
                configure(&mut runtime, target, policy);
                let epoch = runtime.evidence.epoch().0;
                assert_eq!(
                    apply_wire(&mut fixture, &mut runtime, target, "approved", true, epoch)
                        .unwrap(),
                    Outcome::Created(1)
                );
                configure(
                    &mut runtime,
                    target,
                    constraint(Dimension::OutputWriteBytes, 0),
                );
                assert_eq!(
                    apply_wire(&mut fixture, &mut runtime, target, "covered", true, epoch).unwrap(),
                    Outcome::AlreadyCovered
                );
                if mode == PhysicalColumnarDesignMode::Incremental {
                    let after = fixture
                        .database
                        .inspect_lsm_storage(TABLE_ID)
                        .unwrap()
                        .unwrap();
                    assert_eq!(after.memtable_entry_count, before.memtable_entry_count);
                    assert_eq!(after.total_sstable_bytes, before.total_sstable_bytes);
                    assert_eq!(after.write_amplification, before.write_amplification);
                    assert!(
                        after.read_amplification.data_blocks_read
                            > before.read_amplification.data_blocks_read
                    );
                }
                drop(runtime);
                fixture.close();
            }
        }
    }

    #[test]
    fn operator_stale_runtime_evidence_and_recommendation_precede_admission() {
        for target in [
            Target::Index,
            Target::Columnar(PhysicalColumnarDesignMode::Snapshot),
        ] {
            let mut fixture = Fixture::create("p35-precedence");
            let mut runtime = runtime(&mut fixture, true);
            configure(
                &mut runtime,
                target,
                constraint(Dimension::OutputWriteBytes, 0),
            );
            let epoch = runtime.evidence.epoch().0;
            for (matches, expected, code) in [
                (
                    false,
                    epoch,
                    OperatorErrorCodeV6::PhysicalDesignRuntimeChanged,
                ),
                (
                    true,
                    epoch + 1,
                    OperatorErrorCodeV6::PhysicalDesignEvidenceEpochChanged,
                ),
            ] {
                let error = apply_wire(
                    &mut fixture,
                    &mut runtime,
                    target,
                    "approved",
                    matches,
                    expected,
                )
                .unwrap_err();
                assert!(
                    matches!(error, OperatorClientError::Remote(remote) if remote.code == code && remote.admission.is_none() && remote.receipt.is_some())
                );
            }
            runtime.policy.index.minimum_reports = 1000;
            runtime.policy.columnar.minimum_reports = 1000;
            let error = apply_wire(&mut fixture, &mut runtime, target, "approved", true, epoch)
                .unwrap_err();
            let code = match target {
                Target::Index => OperatorErrorCodeV6::PhysicalIndexNotRecommended,
                Target::Columnar(_) => OperatorErrorCodeV6::PhysicalColumnarNotRecommended,
            };
            assert!(
                matches!(error, OperatorClientError::Remote(remote) if remote.code == code && remote.admission.is_none())
            );
            assert!(fixture.database.indexes(TABLE_ID).unwrap().is_empty());
            assert!(fixture.database.inspect_columnar_projections().is_empty());
            drop(runtime);
            fixture.close();
        }
    }

    #[test]
    fn operator_admission_outcome_durability_failure_has_no_admission_payload() {
        for reject in [false, true] {
            for target in [
                Target::Index,
                Target::Columnar(PhysicalColumnarDesignMode::Snapshot),
            ] {
                let mut fixture = Fixture::create("p35-outcome");
                let mut runtime = runtime(&mut fixture, true);
                configure(
                    &mut runtime,
                    target,
                    constraint(
                        Dimension::SourceReadBytes,
                        if reject { 0 } else { u64::MAX },
                    ),
                );
                runtime.fail_next_receipt_outcome_before_write();
                let epoch = runtime.evidence.epoch().0;
                let error = apply_wire(&mut fixture, &mut runtime, target, "approved", true, epoch)
                    .unwrap_err();
                let OperatorClientError::MutationOutcomeUncertain {
                    source,
                    recovery_required: true,
                    receipt: Some(_),
                    ..
                } = error
                else {
                    panic!("receipt outcome uncertainty")
                };
                assert!(
                    matches!(*source, OperatorClientError::Remote(remote) if remote.admission.is_none())
                );
                assert!(
                    receipt_status(&mut fixture.database, &mut runtime)
                        .unwrap()
                        .recovery_required
                );
                drop(runtime);
                fixture.close();
            }
        }
    }
}
