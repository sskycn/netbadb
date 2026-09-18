use super::*;
use crate::authorization::{PrincipalGrants, TablePermissions};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::ColumnId;
use std::path::PathBuf;

fn fixture(name: &str) -> (PathBuf, TableDef, AuthorizationPolicy) {
    let root = std::env::temp_dir().join(format!(
        "netbadb-pg-lifecycle-{name}-{}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let table = TableDef::new(
        TableId(1),
        "items",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    );
    Database::create(root.join("items"), table.clone())
        .unwrap()
        .close()
        .unwrap();
    let policy = AuthorizationPolicy::new(
        TransportKind::PlaintextLoopback,
        Some(PrincipalGrants {
            schema_admin: false,
            tables: vec![TablePermissions::new(TableId(1), true, true, true, false)],
        }),
        Vec::new(),
        &[TableId(1)],
    )
    .unwrap();
    (root, table, policy)
}

fn startup() -> StartupMessage {
    StartupMessage {
        parameters: Default::default(),
    }
}

#[test]
fn abandoned_admission_does_not_leave_a_registered_session() {
    let (root, table, policy) = fixture("abandoned-open");
    let database = Database::open(root.join("items"), table).unwrap();
    let (commands, receiver) = mpsc::channel();
    for _ in 0..1_000 {
        let (reply, abandoned) = mpsc::sync_channel(1);
        drop(abandoned);
        commands
            .send(PgWorkerCommand::Open {
                session_id: 1,
                startup: startup(),
                reply,
            })
            .unwrap();
    }
    let (reply, admitted) = mpsc::sync_channel(1);
    commands
        .send(PgWorkerCommand::Open {
            session_id: 1,
            startup: startup(),
            reply,
        })
        .unwrap();
    let (reply, _stopped) = mpsc::sync_channel(1);
    commands.send(PgWorkerCommand::Shutdown { reply }).unwrap();
    run_pg_worker(
        database,
        SessionPolicy::default(),
        policy,
        None,
        None,
        receiver,
    )
    .unwrap();
    let result = admitted.recv().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        result.is_ok(),
        "abandoned admission retained its session: {result:?}"
    );
}

#[test]
fn disconnect_rollback_failure_stops_worker_before_the_next_request() {
    let (root, table, policy) = fixture("failed-disconnect");
    let path = root.join("items");
    let worker_path = path.clone();
    let worker_table = table.clone();
    let (commands, receiver) = mpsc::channel();
    let join = thread::spawn(move || {
        let database = Database::open(worker_path, worker_table).unwrap();
        run_pg_worker(
            database,
            SessionPolicy::default(),
            policy,
            None,
            None,
            receiver,
        )
    });
    let client = PgWorkerClient { commands };
    client.open(1, startup()).unwrap();
    for sql in ["BEGIN", "INSERT INTO items VALUES (1)"] {
        let messages = client
            .request(1, FrontendMessage::Query(sql.into()))
            .unwrap();
        assert!(
            !messages
                .iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
        );
    }
    // The response is the synchronization boundary: the worker has finished
    // DML and waits on its next command. Damage the exact WAL undo input.
    let wal = netbadb_storage::wal_path(&path);
    let original = std::fs::read(&wal).unwrap();
    let mut damaged = original.clone();
    damaged[48] ^= 0xff;
    std::fs::write(&wal, damaged).unwrap();
    let (reply, closed) = mpsc::sync_channel(1);
    client
        .commands
        .send(PgWorkerCommand::Close {
            session_id: 1,
            reply,
        })
        .unwrap();
    let (reply, following) = mpsc::sync_channel(1);
    // Queue directly so the old implementation also terminates deterministically
    // at Shutdown, rather than leaving a failing test with a live worker.
    let _ = client.commands.send(PgWorkerCommand::Request {
        session_id: 1,
        message: FrontendMessage::Query("SELECT 1".into()),
        reply,
    });
    let (reply, _stopped) = mpsc::sync_channel(1);
    let _ = client.commands.send(PgWorkerCommand::Shutdown { reply });
    assert!(closed.recv().unwrap().is_err());
    let stopped = join.join().unwrap();
    let following = following.recv();
    // Restore the deliberately damaged bytes; recovery must undo, never commit,
    // the abandoned writer, and a subsequent writer must be admitted.
    std::fs::write(&wal, original).unwrap();
    let mut database = Database::open(&path, table).unwrap();
    assert!(
        database
            .query("SELECT id FROM items")
            .unwrap()
            .rows
            .is_empty()
    );
    database.execute("INSERT INTO items VALUES (2)").unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert!(stopped.is_err());
    assert!(
        following.is_err(),
        "worker served a request after failed rollback"
    );
}

#[test]
fn worker_failure_terminates_the_accept_loop_without_external_shutdown() {
    for panic in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let (commands, receiver) = mpsc::channel();
        drop(receiver);
        let join = thread::spawn(move || {
            assert!(!panic, "injected worker panic");
            Err("injected worker failure".into())
        });
        while !join.is_finished() {
            thread::yield_now();
        }
        let worker = PgDatabaseWorker {
            client: PgWorkerClient { commands },
            join,
        };
        let (shutdown, shutdown_rx) = mpsc::channel();
        let (_adaptive, controls) = mpsc::channel();
        let (_design, physical_design_controls) = mpsc::channel();
        let (_failure, operator_failures) = mpsc::channel();
        let (done, result) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let outcome = run_pg_accept_loop(
                listener,
                shutdown_rx,
                worker,
                ServerLimits::default(),
                ServerHostObservationConfig {
                    adaptive: ServerAdaptiveHostConfig {
                        tick_interval: None,
                        controls,
                        operator_failures,
                    },
                    physical_design_controls,
                },
            );
            done.send(outcome).unwrap();
        });
        let outcome = result.recv_timeout(Duration::from_secs(2));
        // Always release/join the old broken loop before asserting failure.
        let _ = shutdown.send(());
        server.join().unwrap();
        assert!(outcome.is_ok(), "dead worker left the listener running");
        assert!(matches!(
            outcome.unwrap(),
            Err(PostgresTcpServerError::WorkerClose(_))
                | Err(PostgresTcpServerError::ThreadPanicked)
        ));
    }
}

#[test]
fn startup_failure_retains_worker_cleanup_error_and_panic() {
    for panic in [false, true] {
        let join = thread::spawn(move || {
            assert!(!panic, "injected worker panic");
            Err("injected close failure".into())
        });
        let error = join_pg_startup_failure(join, PostgresTcpServerError::SessionIdExhausted);
        let PostgresTcpServerError::StartupCleanup { startup, cleanup } = error else {
            panic!("startup lost its cleanup failure");
        };
        assert!(matches!(
            *startup,
            PostgresTcpServerError::SessionIdExhausted
        ));
        if panic {
            assert!(matches!(*cleanup, PostgresTcpServerError::ThreadPanicked));
        } else {
            assert!(matches!(*cleanup, PostgresTcpServerError::WorkerClose(_)));
        }
    }
}

#[test]
fn accept_failure_closes_connections_joins_worker_and_retains_both_errors() {
    use std::io::Read;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    let control = stream.try_clone().unwrap();
    let connection = PgConnection {
        stream: control,
        join: thread::spawn(move || {
            // Cleanup must interrupt the blocked read before joining us.
            assert_eq!(stream.read(&mut [0_u8; 1]).unwrap(), 0);
        }),
    };
    let (commands, receiver) = mpsc::channel();
    let worker = PgDatabaseWorker {
        client: PgWorkerClient { commands },
        join: thread::spawn(move || {
            let PgWorkerCommand::Shutdown { reply } = receiver.recv().unwrap() else {
                panic!("missing shutdown command");
            };
            reply.send(()).unwrap();
            Err("injected cleanup failure".into())
        }),
    };
    let error = finish_pg_accept_loop(
        vec![connection],
        worker,
        Err(PostgresTcpServerError::Accept(io::Error::other(
            "injected accept failure",
        ))),
    )
    .unwrap_err();
    let PostgresTcpServerError::RuntimeCleanup { primary, cleanup } = error else {
        panic!("lost a failure during cleanup");
    };
    assert!(matches!(*primary, PostgresTcpServerError::Accept(_)));
    assert!(matches!(*cleanup, PostgresTcpServerError::WorkerClose(_)));
}

#[test]
fn connection_panic_is_reported_by_reaper() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (stream, _) = listener.accept().unwrap();
    let join = thread::spawn(|| panic!("injected connection panic"));
    while !join.is_finished() {
        thread::yield_now();
    }
    let mut connections = vec![PgConnection { stream, join }];
    assert!(matches!(
        reap_pg_connections(&mut connections),
        Err(PostgresTcpServerError::ThreadPanicked)
    ));
    assert!(connections.is_empty());
}
