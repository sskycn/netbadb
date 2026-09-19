//! Loopback boundary attribution with real server ownership and exact results.
//! Setup, handshake and warmup are outside request latency. No CI time gates.

use std::error::Error;
use std::fs;
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use netbadb_core::{Database, TableStorageCreateSpec};
use netbadb_pgwire::{BackendMessage, PostgresType, encode_text_value, write_backend_message};
use netbadb_protocol::{
    ClientMessage, Frame, ServerMessage, decode_server_frame, encode_server_frame,
    read_server_frame, validate_server_message, write_client_frame,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{PostgresTcpServer, ServerConfig, TcpServer};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
use serde_json::json;

type BenchResult<T> = Result<T, Box<dyn Error>>;

fn main() -> BenchResult<()> {
    let root = std::env::temp_dir().join(format!("netbadb-boundary-{}", std::process::id()));
    fs::create_dir(&root)?;
    let result = run(&root);
    fs::remove_dir_all(&root)?;
    result
}

fn run(root: &Path) -> BenchResult<()> {
    let nullable = match std::env::var("NETBADB_BOUNDARY_NULLS") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "1" => true,
        _ => return Err("NETBADB_BOUNDARY_NULLS must be unset or 1".into()),
    };
    println!(
        "boundary_csv,scenario,clients,requests,min_ns,median_ns,p95_ns,max_ns,requests_per_second"
    );
    for width in [8, 128, 1024] {
        let directory = root.join(format!("w{width}"));
        fs::create_dir(&directory)?;
        let table = TableDef::new(
            TableId(1),
            "items",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "payload",
                    TypeSpec::Physical(PhysicalType::Text),
                )
                .nullable(nullable),
            ],
        );
        let catalog = directory.join("schema");
        let mut database = Database::create_catalog(
            &catalog,
            vec![TableStorageCreateSpec::heap(directory.join("data"), table)],
            None,
        )?;
        let expected = (0..1000_i64)
            .map(|id| {
                vec![
                    ScalarValue::Int64(id),
                    if nullable && id % 2 == 0 {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Text(format!("{id:0width$}"))
                    },
                ]
            })
            .collect::<Vec<_>>();
        let mut transaction = database.begin_transaction_for(TableId(1))?;
        for row in &expected {
            database.insert_into_in(TableId(1), &mut transaction, row)?;
        }
        transaction.commit()?;
        database.create_index(TableId(1), ColumnId(1))?;
        database.analyze(TableId(1))?;
        let cases = [
            ("scalar", "SELECT 1", vec![vec![ScalarValue::Int64(1)]]),
            (
                "point",
                "SELECT id, payload FROM items WHERE id = 500",
                vec![expected[500].clone()],
            ),
            (
                "result",
                if nullable {
                    // Variable-width NULL rows can reuse earlier page space;
                    // request an explicit order for the transport value gate.
                    "SELECT id, payload FROM items ORDER BY id"
                } else {
                    "SELECT id, payload FROM items"
                },
                expected,
            ),
        ];
        for (label, sql, expected) in &cases {
            let plan = database.inspect_statement(sql)?;
            println!("boundary_plan,w{width}_{label},{:?}", plan.plan);
            let mut times = Vec::new();
            for iteration in 0..103 {
                let start = Instant::now();
                let result = database.query(black_box(sql))?;
                let elapsed = start.elapsed();
                if &result.rows != expected {
                    return Err("embedded result differs".into());
                }
                black_box(result);
                if iteration >= 3 {
                    times.push(elapsed);
                }
            }
            report(
                &format!("embedded_w{width}_{label}"),
                1,
                &times,
                times.iter().sum(),
            )?;
            codec(width, label, expected)?;
        }
        database.checkpoint()?;
        database.close()?;
        let manifest = directory.join("server.json");
        fs::write(
            &manifest,
            serde_json::to_vec(&json!({
                "version": 11, "listen": "127.0.0.1:0",
                "authorization": {"local_plaintext": {"tables": [{"table_id":1,"read":true,"write":false,"transaction":false,"analyze":false}]}, "clients":[]},
                "tables":[{"path":"data","id":1,"name":"items","columns":[
                    {"id":1,"name":"id","physical_type":"int64","semantic_type":null,"nullable":false,"primary_key":false},
                    {"id":2,"name":"payload","physical_type":"text","semantic_type":null,"nullable":nullable,"primary_key":false}
                ]}]
            }))?,
        )?;
        let native = TcpServer::new(ServerConfig::from_manifest_path(&manifest)?).start()?;
        for (label, sql, expected) in &cases {
            for clients in [1, 4] {
                network(
                    native.local_addr(),
                    false,
                    width,
                    label,
                    sql,
                    expected,
                    clients,
                )?;
            }
        }
        native.shutdown()?;
        let postgres =
            PostgresTcpServer::new(ServerConfig::from_manifest_path(&manifest)?).start()?;
        for (label, sql, expected) in &cases {
            for clients in [1, 4] {
                network(
                    postgres.local_addr(),
                    true,
                    width,
                    label,
                    sql,
                    expected,
                    clients,
                )?;
            }
        }
        postgres.shutdown()?;
    }
    Ok(())
}

fn report(name: &str, clients: usize, durations: &[Duration], wall: Duration) -> BenchResult<()> {
    let prefix = if std::env::var_os("NETBADB_BOUNDARY_NULLS").is_some() {
        "null_"
    } else {
        ""
    };
    let mut values = durations.iter().map(Duration::as_nanos).collect::<Vec<_>>();
    values.sort_unstable();
    let n = values.len();
    if n == 0 {
        return Err("empty latency sample".into());
    }
    println!(
        "boundary_csv,{prefix}{name},{clients},{n},{},{},{},{},{:.3}",
        values[0],
        values[n / 2],
        values[(n * 95).div_ceil(100) - 1],
        values[n - 1],
        n as f64 / wall.as_secs_f64()
    );
    Ok(())
}

fn codec(width: usize, label: &str, expected: &[Vec<ScalarValue>]) -> BenchResult<()> {
    let native = expected
        .iter()
        .map(|row| ServerMessage::QueryRow {
            values: row.clone(),
        })
        .collect::<Vec<_>>();
    let pg = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| {
                    encode_text_value(
                        value,
                        match value {
                            ScalarValue::Text(_) => PostgresType::Text,
                            _ => PostgresType::Int8,
                        },
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map(BackendMessage::DataRow)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let encoded = native
        .iter()
        .map(|m| encode_server_frame(1, m))
        .collect::<Result<Vec<_>, _>>()?;
    for kind in [
        "native_validate",
        "native_encode",
        "native_decode",
        "pg_convert",
        "pg_encode",
    ] {
        let mut times = Vec::new();
        for iteration in 0..103 {
            let start = Instant::now();
            match kind {
                "native_validate" => {
                    for message in &native {
                        validate_server_message(black_box(message))?;
                    }
                }
                "native_encode" => {
                    for message in &native {
                        black_box(encode_server_frame(1, message)?);
                    }
                }
                "native_decode" => {
                    for frame in &encoded {
                        black_box(decode_server_frame(frame)?);
                    }
                }
                "pg_convert" => {
                    for row in expected {
                        for value in row {
                            black_box(encode_text_value(
                                value,
                                match value {
                                    ScalarValue::Text(_) => PostgresType::Text,
                                    _ => PostgresType::Int8,
                                },
                            )?);
                        }
                    }
                }
                "pg_encode" => {
                    let mut output = Vec::new();
                    for message in &pg {
                        write_backend_message(&mut output, message)?;
                    }
                    black_box(output);
                }
                _ => return Err("unknown codec benchmark".into()),
            }
            if iteration >= 3 {
                times.push(start.elapsed());
            }
        }
        report(
            &format!("{kind}_w{width}_{label}"),
            1,
            &times,
            times.iter().sum(),
        )?;
    }
    for (bytes, expected) in encoded.iter().zip(&native) {
        if decode_server_frame(bytes)?.message != *expected {
            return Err("codec round trip differs".into());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn network(
    address: SocketAddr,
    postgres: bool,
    width: usize,
    label: &str,
    sql: &str,
    expected: &[Vec<ScalarValue>],
    clients: usize,
) -> BenchResult<()> {
    let mut connections = Vec::new();
    for _ in 0..clients {
        let mut connection = Connection::open(address, postgres)?;
        for _ in 0..3 {
            connection.query(sql, expected)?;
        }
        connections.push(connection);
    }
    let barrier = Arc::new(Barrier::new(clients + 1));
    let expected = Arc::new(expected.to_vec());
    let handles = connections
        .into_iter()
        .map(|mut connection| {
            let barrier = Arc::clone(&barrier);
            let expected = Arc::clone(&expected);
            let sql = sql.to_owned();
            std::thread::spawn(move || -> Result<Vec<Duration>, String> {
                let result = (|| -> BenchResult<Vec<Duration>> {
                    barrier.wait();
                    let mut durations = Vec::new();
                    for _ in 0..100 {
                        let start = Instant::now();
                        connection.query(&sql, &expected)?;
                        durations.push(start.elapsed());
                    }
                    Ok(durations)
                })();
                result.map_err(|error| error.to_string())
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let start = Instant::now();
    let mut durations = Vec::new();
    for handle in handles {
        durations.extend(
            handle
                .join()
                .map_err(|_| "benchmark client panicked")?
                .map_err(|error| format!("benchmark client: {error}"))?,
        );
    }
    report(
        &format!(
            "{}_w{width}_{label}",
            if postgres { "pg" } else { "native" }
        ),
        clients,
        &durations,
        start.elapsed(),
    )
}

struct Connection {
    stream: TcpStream,
    postgres: bool,
    request: u64,
}

impl Connection {
    fn open(address: SocketAddr, postgres: bool) -> BenchResult<Self> {
        let mut stream = TcpStream::connect(address)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.set_write_timeout(Some(Duration::from_secs(30)))?;
        if postgres {
            let mut startup = 196_608_u32.to_be_bytes().to_vec();
            startup.extend_from_slice(b"user\0benchmark\0database\0benchmark\0\0");
            stream.write_all(&u32::try_from(startup.len() + 4)?.to_be_bytes())?;
            stream.write_all(&startup)?;
            loop {
                let (tag, _) = pg_message(&mut stream)?;
                if tag == b'Z' {
                    break;
                }
                if tag == b'E' {
                    return Err("PG startup failed".into());
                }
            }
        } else {
            write_client_frame(
                &mut stream,
                &Frame {
                    request_id: 1,
                    message: ClientMessage::Hello,
                },
            )?;
            if !matches!(
                read_server_frame(&mut stream)?.ok_or("native EOF")?.message,
                ServerMessage::HelloAck { .. }
            ) {
                return Err("native handshake failed".into());
            }
        }
        Ok(Self {
            stream,
            postgres,
            request: 1,
        })
    }

    fn query(&mut self, sql: &str, expected: &[Vec<ScalarValue>]) -> BenchResult<()> {
        self.request += 1;
        if self.postgres {
            let mut bytes = vec![b'Q'];
            bytes.extend_from_slice(&u32::try_from(sql.len() + 5)?.to_be_bytes());
            bytes.extend_from_slice(sql.as_bytes());
            bytes.push(0);
            self.stream.write_all(&bytes)?;
            let mut row = 0;
            loop {
                let (tag, payload) = pg_message(&mut self.stream)?;
                if tag == b'D' {
                    let values = expected.get(row).ok_or("extra PG row")?;
                    let mut cursor = payload.as_slice();
                    let mut count = [0; 2];
                    cursor.read_exact(&mut count)?;
                    if usize::from(u16::from_be_bytes(count)) != values.len() {
                        return Err("PG column count differs".into());
                    }
                    for value in values {
                        let mut length = [0; 4];
                        cursor.read_exact(&mut length)?;
                        let length = i32::from_be_bytes(length);
                        if matches!(value, ScalarValue::Null) {
                            if length != -1 {
                                return Err("PG NULL encoding differs".into());
                            }
                            continue;
                        }
                        let text = match value {
                            ScalarValue::Int64(i) => i.to_string(),
                            ScalarValue::Text(t) => t.clone(),
                            _ => return Err("unexpected benchmark scalar".into()),
                        };
                        let length = usize::try_from(length)?;
                        if cursor.get(..length) != Some(text.as_bytes()) {
                            return Err("PG scalar differs".into());
                        }
                        cursor = &cursor[length..];
                    }
                    if !cursor.is_empty() {
                        return Err("PG trailing row bytes".into());
                    }
                    row += 1;
                } else if tag == b'E' {
                    return Err(format!("PG error: {}", String::from_utf8_lossy(&payload)).into());
                } else if tag == b'Z' {
                    if row != expected.len() {
                        return Err("PG row count differs".into());
                    }
                    break;
                }
            }
        } else {
            write_client_frame(
                &mut self.stream,
                &Frame {
                    request_id: self.request,
                    message: ClientMessage::Execute { sql: sql.into() },
                },
            )?;
            let mut row = 0;
            loop {
                let frame = read_server_frame(&mut self.stream)?.ok_or("native EOF")?;
                if frame.request_id != self.request {
                    return Err("native request mismatch".into());
                }
                match frame.message {
                    ServerMessage::QueryStart { .. } => {}
                    ServerMessage::QueryRow { values } => {
                        if expected.get(row) != Some(&values) {
                            return Err("native scalar differs".into());
                        }
                        row += 1;
                    }
                    ServerMessage::QueryEnd { row_count }
                        if row_count == u64::try_from(expected.len())? && row == expected.len() =>
                    {
                        break;
                    }
                    message => return Err(format!("unexpected native response {message:?}").into()),
                }
            }
        }
        Ok(())
    }
}

fn pg_message(stream: &mut TcpStream) -> BenchResult<(u8, Vec<u8>)> {
    let mut header = [0; 5];
    stream.read_exact(&mut header)?;
    let size = usize::try_from(u32::from_be_bytes(header[1..].try_into()?))?;
    if !(4..=16 * 1024 * 1024).contains(&size) {
        return Err("invalid PG response length".into());
    }
    let mut payload = vec![0; size - 4];
    stream.read_exact(&mut payload)?;
    Ok((header[0], payload))
}
