// Included by deployment.rs to share its real process/readiness/signal harness.
mod phase35 {
    use super::*;
    use netbadb_core::{DatabaseCoordinatorConfig, TableStorageCreateSpec};
    use netbadb_server::{
        OperatorClientError, OperatorErrorCodeV7, OperatorPhysicalColumnarDesignModeV7 as Mode,
        OperatorPhysicalDesignMutationAdmissionDimensionV7 as AdmissionDimension,
        OperatorPhysicalDesignMutationAdmissionModeV7 as Admission,
        OperatorPhysicalDesignMutationAdmissionRejectionV7 as AdmissionRejection,
        OperatorPhysicalDesignMutationReceiptOutcomeV7 as ReceiptOutcome,
    };
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};

    fn policy(field: &str, maximum: u64) -> serde_json::Value {
        let mut value = json!({"mode":"component_limits"});
        for dimension in [
            "source_work_units",
            "source_read_bytes",
            "prerequisite_work_units",
            "prerequisite_read_bytes",
            "prerequisite_write_bytes",
            "output_write_bytes",
        ] {
            value[dimension] = json!({"kind":"unconstrained"});
        }
        value[field] = json!({"kind":"at_most","maximum":maximum});
        value
    }

    fn fixture() -> Fixture {
        let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("netbadbd-p35-{}-{sequence}", std::process::id()));
        std::fs::create_dir_all(directory.join("columnar")).unwrap();
        let table = TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "category",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
                ColumnDef::new(
                    ColumnId(3),
                    "other",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
            ],
        );
        let heap = directory.join("users.ndb");
        let mut database = Database::create_catalog(
            directory.join("catalog"),
            vec![TableStorageCreateSpec::heap(&heap, table.clone())],
            Some(
                DatabaseCoordinatorConfig::new(directory.join("coordinator"))
                    .with_global_visibility(),
            ),
        )
        .unwrap();
        database.enable_change_stream(TableId(1)).unwrap();
        database
            .execute("INSERT INTO users VALUES (1, 7, 9)")
            .unwrap();
        database.close().unwrap();
        let socket = PathBuf::from(format!(
            "/tmp/netbadbd-p35-{}-{sequence}.sock",
            std::process::id()
        ));
        let example = include_str!("../../../../docs/server-manifest-v11.md")
            .split("```json\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap();
        let mut source: serde_json::Value = serde_json::from_str(example).unwrap();
        source["listen"] = json!("127.0.0.1:0");
        source["authorization"]["local_plaintext"] = json!({"schema_admin":true,"tables":[{"table_id":1,"read":true,"write":true,"transaction":true,"analyze":true}]});
        source["tables"][0]["path"] = json!("users.ndb");
        source["tables"][0]["columns"] = json!([
            {"id":1,"name":"id","physical_type":"int64","semantic_type":null,"nullable":false,"primary_key":false},
            {"id":2,"name":"category","physical_type":"int64","semantic_type":null,"nullable":false,"primary_key":false},
            {"id":3,"name":"other","physical_type":"int64","semantic_type":null,"nullable":false,"primary_key":false}
        ]);
        source["physical_design"]["columnar_apply"] =
            json!({"root":"columnar","allow_snapshot":true,"allow_incremental":true});
        source["physical_design"]["mutation_receipts"]["path"] = json!("receipts.nbmr");
        source["operator"]["unix_socket"] = json!(socket);
        source["operator"]["allow_physical_design_receipt_read"] = json!(true);
        source["operator"]["physical_index_admission"] = policy("source_read_bytes", 0);
        source["operator"]["physical_columnar_snapshot_admission"] =
            policy("output_write_bytes", 0);
        source["operator"]["physical_columnar_incremental_admission"] =
            policy("source_read_bytes", u64::MAX);
        let manifest = directory.join("server.json");
        std::fs::write(&manifest, serde_json::to_vec(&source).unwrap()).unwrap();
        Fixture {
            directory,
            manifest,
            heap,
            table,
            socket: Some(socket),
        }
    }

    fn address(ready: &str) -> SocketAddr {
        ready
            .split("listener on ")
            .nth(1)
            .unwrap()
            .split(',')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }

    fn query(address: SocketAddr, postgres: bool, sql: &str) {
        if !postgres {
            let mut client =
                netbadb_client::Client::connect(netbadb_client::Config::new(address.to_string()))
                    .unwrap();
            client.query(sql).unwrap().close().unwrap();
            client.close().unwrap();
            return;
        }
        if let Some(psql) = std::env::var_os("NETBADB_TEST_PSQL") {
            let result = Command::new(psql)
                .env("DYLD_LIBRARY_PATH", "/opt/local/lib/icu/lib")
                .args([
                    "-X",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &address.port().to_string(),
                    "-U",
                    "netbadb",
                    "-d",
                    "netbadb",
                    "-c",
                    sql,
                ])
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            return;
        }
        let mut stream = TcpStream::connect(address).unwrap();
        stream.set_read_timeout(Some(PROCESS_TIMEOUT)).unwrap();
        let mut startup = 196608_u32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0netbadb\0database\0netbadb\0\0");
        stream
            .write_all(&((startup.len() + 4) as u32).to_be_bytes())
            .unwrap();
        stream.write_all(&startup).unwrap();
        read_ready(&mut stream);
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        stream.write_all(b"Q").unwrap();
        stream
            .write_all(&((body.len() + 4) as u32).to_be_bytes())
            .unwrap();
        stream.write_all(&body).unwrap();
        read_ready(&mut stream);
        stream.write_all(b"X\0\0\0\x04").unwrap();
    }
    fn read_ready(stream: &mut TcpStream) {
        loop {
            let mut header = [0; 5];
            stream.read_exact(&mut header).unwrap();
            let len = u32::from_be_bytes(header[1..].try_into().unwrap());
            let mut body = vec![0; usize::try_from(len - 4).unwrap()];
            stream.read_exact(&mut body).unwrap();
            assert_ne!(header[0], b'E', "{}", String::from_utf8_lossy(&body));
            if header[0] == b'Z' {
                assert_eq!(body, b"I");
                break;
            }
        }
    }
    fn rejected(
        error: OperatorClientError,
        operator: &ServerOperatorClient<'_>,
    ) -> AdmissionRejection {
        let OperatorClientError::Remote(remote) = error else {
            panic!("definite rejection")
        };
        assert_eq!(
            remote.code,
            OperatorErrorCodeV7::PhysicalDesignMutationAdmissionRejected
        );
        let admission = remote.admission.unwrap();
        let reference = remote.receipt.unwrap();
        let page = operator
            .physical_design_mutation_receipts(None, 128)
            .unwrap();
        assert_eq!(page.journal_incarnation, reference.journal_incarnation);
        let receipt = page
            .receipts
            .iter()
            .find(|r| r.receipt.receipt_id == reference.receipt_id)
            .unwrap();
        assert_eq!(receipt.outcome, ReceiptOutcome::Rejected);
        admission
    }

    fn run(postgres: bool) {
        let fixture = fixture();
        let mut daemon = DaemonProcess::spawn(&fixture.manifest, postgres);
        let addr = address(&daemon.wait_for_readiness());
        query(addr, postgres, "SELECT id FROM users WHERE category = 7");
        query(addr, postgres, "SELECT id FROM users");
        let config = ServerConfig::from_manifest_path(&fixture.manifest).unwrap();
        let operator = ServerOperatorClient::new(config.operator_config().unwrap());
        let before = operator.status().unwrap();
        let design = before.physical_design.unwrap();
        assert_eq!(
            design.physical_index_apply.admission,
            Admission::from(config.operator_config().unwrap().physical_index_admission())
        );
        assert_eq!(
            design.physical_columnar_apply.snapshot_admission,
            Admission::from(
                config
                    .operator_config()
                    .unwrap()
                    .physical_columnar_snapshot_admission()
            )
        );
        assert_eq!(
            design.physical_columnar_apply.incremental_admission,
            Admission::from(
                config
                    .operator_config()
                    .unwrap()
                    .physical_columnar_incremental_admission()
            )
        );
        let old_token = design.physical_index_apply.runtime_token.unwrap();
        let epoch = design.evidence.epoch;
        let _ = rejected(
            operator
                .apply_physical_index(old_token.clone(), epoch, 1, 2, "by_category")
                .unwrap_err(),
            &operator,
        );
        let columnar_rejection = rejected(
            operator
                .apply_physical_columnar(
                    old_token.clone(),
                    epoch,
                    1,
                    vec![1],
                    Mode::Snapshot,
                    "snapshot",
                )
                .unwrap_err(),
            &operator,
        );
        assert!(matches!(
            columnar_rejection,
            AdmissionRejection::LimitExceeded {
                dimension: AdmissionDimension::OutputWriteBytes,
                conservative_bound,
                maximum: 0,
            } if conservative_bound > 0
        ));
        assert_eq!(
            operator.status().unwrap().physical_design.unwrap().evidence,
            design.evidence
        );
        query(addr, postgres, "SELECT id FROM users WHERE category = 7");
        query(addr, postgres, "SELECT id FROM users");
        daemon.send_signal("SIGTERM");
        assert!(daemon.wait().0.success());
        assert!(!fixture.socket.as_ref().unwrap().exists());
        let mut source: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&fixture.manifest).unwrap()).unwrap();
        source["operator"]["physical_index_admission"] = policy("source_read_bytes", u64::MAX);
        source["operator"]["physical_columnar_snapshot_admission"] =
            policy("source_read_bytes", u64::MAX);
        std::fs::write(&fixture.manifest, serde_json::to_vec(&source).unwrap()).unwrap();
        let mut daemon = DaemonProcess::spawn(&fixture.manifest, postgres);
        let addr = address(&daemon.wait_for_readiness());
        let config = ServerConfig::from_manifest_path(&fixture.manifest).unwrap();
        let operator = ServerOperatorClient::new(config.operator_config().unwrap());
        let error = operator
            .apply_physical_index(old_token, epoch, 1, 2, "by_category")
            .unwrap_err();
        assert!(
            matches!(error,OperatorClientError::Remote(remote) if remote.code==OperatorErrorCodeV7::PhysicalDesignRuntimeChanged && remote.admission.is_none())
        );
        query(addr, postgres, "SELECT id FROM users WHERE category = 7");
        query(addr, postgres, "SELECT id FROM users");
        query(addr, postgres, "SELECT other FROM users");
        let report = operator.physical_design_recommendations().unwrap();
        let token = report.runtime_token.unwrap();
        let epoch = report.report.evidence_epoch;
        let index = operator
            .apply_physical_index(token.clone(), epoch, 1, 2, "by_category")
            .unwrap();
        assert!(index.receipt.is_some());
        let snapshot = operator
            .apply_physical_columnar(token.clone(), epoch, 1, vec![1], Mode::Snapshot, "snapshot")
            .unwrap();
        assert!(snapshot.receipt.is_some());
        let incremental = operator
            .apply_physical_columnar(
                token.clone(),
                epoch,
                1,
                vec![3],
                Mode::Incremental,
                "incremental",
            )
            .unwrap();
        assert!(incremental.receipt.is_some());
        query(addr, postgres, "SELECT id FROM users WHERE category = 7");
        query(addr, postgres, "SELECT id FROM users");
        daemon.send_signal("SIGTERM");
        assert!(daemon.wait().0.success());
        source["operator"]["physical_index_admission"] = policy("output_write_bytes", 0);
        source["operator"]["physical_columnar_snapshot_admission"] =
            policy("output_write_bytes", 0);
        std::fs::write(&fixture.manifest, serde_json::to_vec(&source).unwrap()).unwrap();
        let mut daemon = DaemonProcess::spawn(&fixture.manifest, postgres);
        daemon.wait_for_readiness();
        let config = ServerConfig::from_manifest_path(&fixture.manifest).unwrap();
        let operator = ServerOperatorClient::new(config.operator_config().unwrap());
        assert!(matches!(
            operator
                .apply_physical_index(token.clone(), epoch, 1, 2, "by_category")
                .unwrap()
                .outcome,
            netbadb_server::OperatorPhysicalIndexApplyOutcomeV7::AlreadyApplied { .. }
        ));
        assert!(matches!(
            operator
                .apply_physical_columnar(token, epoch, 1, vec![1], Mode::Snapshot, "snapshot")
                .unwrap()
                .outcome,
            netbadb_server::OperatorPhysicalColumnarApplyOutcomeV7::AlreadyApplied { .. }
        ));
        daemon.send_signal("SIGTERM");
        assert!(daemon.wait().0.success());
        assert!(!fixture.socket.as_ref().unwrap().exists());
        fixture.cleanup();
    }
    #[test]
    fn phase35_native_daemon_admission_and_restart() {
        run(false);
    }
    #[test]
    fn phase35_postgres_daemon_admission_and_restart() {
        run(true);
    }
}
