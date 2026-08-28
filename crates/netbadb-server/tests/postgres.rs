use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};

use netbadb_core::Database;
use netbadb_pgwire::{CANCEL_REQUEST_CODE, PROTOCOL_VERSION_3, SSL_REQUEST_CODE};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{PostgresTcpServer, ServerConfig};
use netbadb_types::{ColumnId, PhysicalType, TableId};

fn test_directory(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("netbadb-postgres-{name}-{}", std::process::id()))
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

fn users_table() -> TableDef {
    TableDef::new(
        TableId(1),
        "users",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64))
                .primary_key(true),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text))
                .nullable(true),
            ColumnDef::new(
                ColumnId(3),
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            ),
        ],
    )
}

fn start_server(name: &str) -> (PathBuf, netbadb_server::PostgresServerHandle) {
    let directory = test_directory(name);
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    Database::create(directory.join("users.ndb"), users_table())
        .unwrap()
        .close()
        .unwrap();
    let manifest = directory.join("server.json");
    std::fs::write(
        &manifest,
        r#"{
            "version": 4,
            "listen": "127.0.0.1:0",
            "authorization": {
                "local_plaintext": {
                    "tables": [{
                        "table_id": 1,
                        "read": true,
                        "write": true,
                        "transaction": true,
                        "analyze": false
                    }]
                },
                "clients": []
            },
            "tables": [{
                "path": "users.ndb",
                "id": 1,
                "name": "users",
                "columns": [
                    {"id":1,"name":"id","physical_type":"int64","semantic_type":null,"nullable":false,"primary_key":true},
                    {"id":2,"name":"name","physical_type":"text","semantic_type":null,"nullable":true,"primary_key":false},
                    {"id":3,"name":"active","physical_type":"bool","semantic_type":null,"nullable":false,"primary_key":false}
                ]
            }]
        }"#,
    )
    .unwrap();
    let server = PostgresTcpServer::new(ServerConfig::from_manifest_path(&manifest).unwrap())
        .start()
        .unwrap();
    (directory, server)
}

fn startup(stream: &mut TcpStream) {
    let mut ssl = 8_i32.to_be_bytes().to_vec();
    ssl.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
    stream.write_all(&ssl).unwrap();
    let mut response = [0_u8; 1];
    stream.read_exact(&mut response).unwrap();
    assert_eq!(response, [b'N']);

    let mut payload = PROTOCOL_VERSION_3.to_be_bytes().to_vec();
    payload.extend_from_slice(b"user\0netbadb\0database\0test\0application_name\0integration\0unknown_parameter\0ignored\0\0");
    let mut frame = i32::try_from(payload.len() + 4)
        .unwrap()
        .to_be_bytes()
        .to_vec();
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).unwrap();
    let messages = read_until_ready(stream);
    assert_eq!(messages.first().map(|message| message.0), Some(b'R'));
    assert_eq!(messages.last().map(|message| message.0), Some(b'Z'));
    assert_eq!(messages.last().unwrap().1, [b'I']);
}

fn frontend(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![tag];
    frame.extend_from_slice(&i32::try_from(payload.len() + 4).unwrap().to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(&frontend(b'Q', &payload)).unwrap();
    read_until_ready(stream)
}

fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag).unwrap();
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).unwrap();
    let length = usize::try_from(i32::from_be_bytes(length)).unwrap();
    assert!(length >= 4);
    let mut payload = vec![0_u8; length - 4];
    stream.read_exact(&mut payload).unwrap();
    (tag[0], payload)
}

fn read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    loop {
        let message = read_message(stream);
        let ready = message.0 == b'Z';
        messages.push(message);
        if ready {
            return messages;
        }
    }
}

fn error_sqlstate(payload: &[u8]) -> Option<&str> {
    let mut position = 0;
    while position < payload.len() && payload[position] != 0 {
        let tag = payload[position];
        position += 1;
        let end = payload[position..].iter().position(|byte| *byte == 0)? + position;
        if tag == b'C' {
            return std::str::from_utf8(&payload[position..end]).ok();
        }
        position = end + 1;
    }
    None
}

#[test]
fn real_tcp_startup_simple_query_rows_transactions_and_errors() {
    let (directory, server) = start_server("simple");
    let mut stream = TcpStream::connect(server.local_addr()).unwrap();
    startup(&mut stream);

    let insert = query(
        &mut stream,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', true)",
    );
    assert!(
        insert
            .iter()
            .any(|message| message.0 == b'C' && message.1.starts_with(b"INSERT 0 1"))
    );

    let select = query(
        &mut stream,
        "SELECT id, name, active FROM users WHERE id = 1",
    );
    assert_eq!(select.iter().filter(|message| message.0 == b'T').count(), 1);
    let row = select.iter().find(|message| message.0 == b'D').unwrap();
    assert!(row.1.windows(3).any(|bytes| bytes == b"Ada"));
    assert!(row.1.ends_with(b"t"));

    let transaction = query(
        &mut stream,
        "BEGIN; UPDATE users SET name = NULL WHERE id = 1; COMMIT;",
    );
    assert_eq!(
        transaction
            .iter()
            .filter(|message| message.0 == b'C')
            .count(),
        3
    );
    assert_eq!(transaction.last().unwrap().1, [b'I']);
    let null_row = query(&mut stream, "SELECT name FROM users WHERE id = 1");
    let data = &null_row.iter().find(|message| message.0 == b'D').unwrap().1;
    assert_eq!(&data[2..], &[255, 255, 255, 255]);

    assert_eq!(query(&mut stream, "BEGIN").last().unwrap().1, [b'T']);
    let failed = query(&mut stream, "SELECT missing FROM users");
    assert_eq!(failed.last().unwrap().1, [b'E']);
    assert_eq!(
        error_sqlstate(&failed.iter().find(|message| message.0 == b'E').unwrap().1),
        Some("42703")
    );
    let blocked = query(&mut stream, "SELECT id FROM users");
    assert_eq!(
        error_sqlstate(&blocked.iter().find(|message| message.0 == b'E').unwrap().1),
        Some("25P02")
    );
    assert_eq!(query(&mut stream, "ROLLBACK").last().unwrap().1, [b'I']);

    let compatibility = query(&mut stream, "SELECT current_database()");
    assert!(
        compatibility
            .iter()
            .any(|message| message.0 == b'D' && message.1.ends_with(b"test"))
    );

    stream.write_all(&frontend(b'X', &[])).unwrap();
    drop(stream);
    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn real_tcp_extended_query_manages_named_statement_and_portal() {
    let (directory, server) = start_server("extended");
    let mut stream = TcpStream::connect(server.local_addr()).unwrap();
    startup(&mut stream);
    let _ = query(
        &mut stream,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', true)",
    );
    let _ = query(
        &mut stream,
        "INSERT INTO users (id, name, active) VALUES (2, 'Lin', false)",
    );

    let mut parse = b"find_user\0SELECT id, name FROM users ORDER BY id\0".to_vec();
    parse.extend_from_slice(&0_i16.to_be_bytes());
    stream.write_all(&frontend(b'P', &parse)).unwrap();

    let mut bind = b"one\0find_user\0".to_vec();
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    stream.write_all(&frontend(b'B', &bind)).unwrap();

    stream.write_all(&frontend(b'D', b"Pone\0")).unwrap();
    let mut execute = b"one\0".to_vec();
    execute.extend_from_slice(&1_u32.to_be_bytes());
    stream.write_all(&frontend(b'E', &execute)).unwrap();
    stream.write_all(&frontend(b'S', &[])).unwrap();

    let messages = read_until_ready(&mut stream);
    let tags = messages.iter().map(|message| message.0).collect::<Vec<_>>();
    assert_eq!(tags, [b'1', b'2', b'T', b'D', b's', b'Z']);
    assert!(
        messages
            .iter()
            .find(|message| message.0 == b'D')
            .unwrap()
            .1
            .windows(3)
            .any(|bytes| bytes == b"Ada")
    );

    let mut execute = b"one\0".to_vec();
    execute.extend_from_slice(&1_u32.to_be_bytes());
    stream.write_all(&frontend(b'E', &execute)).unwrap();
    stream.write_all(&frontend(b'S', &[])).unwrap();
    let remainder = read_until_ready(&mut stream);
    assert_eq!(
        remainder
            .iter()
            .map(|message| message.0)
            .collect::<Vec<_>>(),
        [b'D', b'C', b'Z']
    );
    assert!(
        remainder
            .iter()
            .find(|message| message.0 == b'D')
            .unwrap()
            .1
            .windows(3)
            .any(|bytes| bytes == b"Lin")
    );

    stream.write_all(&frontend(b'C', b"Pone\0")).unwrap();
    stream.write_all(&frontend(b'C', b"Sfind_user\0")).unwrap();
    stream.write_all(&frontend(b'S', &[])).unwrap();
    let close = read_until_ready(&mut stream);
    assert_eq!(
        close.iter().map(|message| message.0).collect::<Vec<_>>(),
        [b'3', b'3', b'Z']
    );

    let mut parameterized = b"by_id\0SELECT id FROM users WHERE id = $1\0".to_vec();
    parameterized.extend_from_slice(&1_i16.to_be_bytes());
    parameterized.extend_from_slice(&20_u32.to_be_bytes());
    stream.write_all(&frontend(b'P', &parameterized)).unwrap();
    stream.write_all(&frontend(b'B', &bind)).unwrap();
    stream.write_all(&frontend(b'S', &[])).unwrap();
    let rejected = read_until_ready(&mut stream);
    assert_eq!(
        rejected.iter().map(|message| message.0).collect::<Vec<_>>(),
        [b'E', b'Z']
    );
    assert_eq!(error_sqlstate(&rejected[0].1), Some("0A000"));

    stream.write_all(&frontend(b'X', &[])).unwrap();
    drop(stream);
    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn cancel_request_is_accepted_without_creating_a_session() {
    let (directory, server) = start_server("cancel");
    let mut stream = TcpStream::connect(server.local_addr()).unwrap();
    let mut cancel = 16_i32.to_be_bytes().to_vec();
    cancel.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    cancel.extend_from_slice(&1_i32.to_be_bytes());
    cancel.extend_from_slice(&2_i32.to_be_bytes());
    stream.write_all(&cancel).unwrap();
    let mut byte = [0_u8; 1];
    assert_eq!(stream.read(&mut byte).unwrap(), 0);
    server.shutdown().unwrap();
    cleanup(&directory);
}
