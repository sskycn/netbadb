package netbadb

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func readClientTestFrame(reader io.Reader) (uint64, uint16, []byte, error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, 0, nil, err
	}
	length := binary.LittleEndian.Uint32(header[12:16])
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, 0, nil, err
	}
	return binary.LittleEndian.Uint64(header[16:24]), binary.LittleEndian.Uint16(header[6:8]), payload, nil
}

func writeTestMessage(t *testing.T, writer io.Writer, requestID uint64, message serverMessage) {
	t.Helper()
	frame, err := encodeServerTestFrame(requestID, message)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write(frame)
}

func pipeClient() (*Client, net.Conn) {
	client, server := net.Pipe()
	return &Client{conn: client, nextRequestID: 1, serverInfo: ServerInfo{MaxFramePayload: maxFramePayload}}, server
}

type shortWriteConn struct{ net.Conn }

func (conn shortWriteConn) Write(bytes []byte) (int, error) {
	if len(bytes) > 3 {
		bytes = bytes[:3]
	}
	return conn.Conn.Write(bytes)
}

func TestClientWritesCompleteFramesAcrossShortWrites(t *testing.T) {
	rawClient, server := net.Pipe()
	client := &Client{conn: shortWriteConn{rawClient}, nextRequestID: 1, serverInfo: ServerInfo{MaxFramePayload: maxFramePayload}}
	defer client.Close()
	defer server.Close()
	go func() {
		id, kind, _, err := readClientTestFrame(server)
		if err == nil && kind == kindClientPing {
			writeTestMessage(t, server, id, serverMessage{kind: kindServerPong})
		}
	}()
	if err := client.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPlaintextSafetyUsesConnectedRemoteIP(t *testing.T) {
	if err := validatePlaintextRemote(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7878}); err != nil {
		t.Fatal(err)
	}
	err := validatePlaintextRemote(&net.TCPAddr{IP: net.ParseIP("203.0.113.1"), Port: 7878})
	var remote *PlaintextRemoteNotAllowedError
	if !errors.As(err, &remote) {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestInsecureSkipVerifyIsRejectedBeforeDial(t *testing.T) {
	_, err := Dial(context.Background(), Config{Address: "not-dialed.invalid:7878", TLS: &tls.Config{InsecureSkipVerify: true}})
	var state *ClientStateError
	if !errors.As(err, &state) {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestStreamingRowsDrainAndMonotonicRequestIDs(t *testing.T) {
	client, server := pipeClient()
	defer client.Close()
	defer server.Close()
	done := make(chan error, 1)
	go func() {
		id, kind, _, err := readClientTestFrame(server)
		if err != nil {
			done <- err
			return
		}
		if id != 1 || kind != kindClientExecute {
			done <- errors.New("unexpected first request")
			return
		}
		writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryStart, columns: []ResultColumn{{Name: "id", Type: SemanticType{Physical: PhysicalTypeUInt64}}, {Name: "name", Type: SemanticType{Physical: PhysicalTypeText}, Nullable: true}}})
		writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryRow, values: []Value{UInt64Value(1), TextValue("A")}})
		writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryRow, values: []Value{UInt64Value(2), Null()}})
		writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryEnd, count: 2})
		id, kind, _, err = readClientTestFrame(server)
		if err != nil {
			done <- err
			return
		}
		if id != 2 || kind != kindClientPing {
			done <- errors.New("unexpected second request")
			return
		}
		writeTestMessage(t, server, id, serverMessage{kind: kindServerPong})
		done <- nil
	}()

	rows, err := client.Query(context.Background(), "SELECT id, name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if got := rows.Columns(); len(got) != 2 || got[0].Name != "id" {
		t.Fatalf("columns = %#v", got)
	}
	if err := client.Ping(context.Background()); !errors.Is(err, ErrRowsOpen) {
		t.Fatalf("Ping while rows open = %v", err)
	}
	if !rows.Next() {
		t.Fatalf("first Next: %v", rows.Err())
	}
	if got := rows.Values(); len(got) != 2 || got[0] != UInt64Value(1) {
		t.Fatalf("values = %#v", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRowsRejectShapeTypeNullabilityAndCountMismatches(t *testing.T) {
	cases := []struct {
		name string
		row  []Value
		end  uint64
	}{
		{"shape", nil, 1},
		{"type", []Value{TextValue("wrong")}, 1},
		{"nullability", []Value{Null()}, 1},
		{"row count", []Value{UInt64Value(1)}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, server := pipeClient()
			defer server.Close()
			go func() {
				id, _, _, _ := readClientTestFrame(server)
				writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryStart, columns: []ResultColumn{{Name: "id", Type: SemanticType{Physical: PhysicalTypeUInt64}}}})
				writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryRow, values: tc.row})
				writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryEnd, count: tc.end})
			}()
			rows, err := client.Query(context.Background(), "SELECT id FROM users")
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
			}
			var protocol *ProtocolError
			if !errors.As(rows.Err(), &protocol) {
				t.Fatalf("Rows.Err = %T %v", rows.Err(), rows.Err())
			}
			if !client.closed {
				t.Fatal("protocol violation did not close client")
			}
		})
	}
}

func TestExecDrainsUnexpectedQueryAndQueryConsumesAffectedRows(t *testing.T) {
	client, server := pipeClient()
	defer client.Close()
	defer server.Close()
	go func() {
		id, _, _, _ := readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryStart, columns: []ResultColumn{}})
		writeTestMessage(t, server, id, serverMessage{kind: kindServerQueryEnd, count: 0})
		id, _, _, _ = readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerPong})
		id, _, _, _ = readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerAffectedRows, count: 1})
	}()
	_, err := client.Exec(context.Background(), "SELECT id FROM users")
	var expectedAffected *ExpectedAffectedRowsError
	if !errors.As(err, &expectedAffected) {
		t.Fatalf("Exec error = %T %v", err, err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(context.Background(), "INSERT INTO users (id) VALUES (1)"); err == nil {
		t.Fatal("Query accepted AffectedRows")
	}
	if client.closed {
		t.Fatal("complete response mismatch poisoned connection")
	}
}

func TestTransactionRemoteStatesControlLocalLifecycle(t *testing.T) {
	client, server := pipeClient()
	defer client.Close()
	defer server.Close()
	go func() {
		id, _, _, _ := readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerTransactionStarted})
		id, _, _, _ = readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerError, remoteError: &RemoteError{Code: ErrorCodeCompile, TransactionState: TransactionStateActive, Message: "bad SQL"}})
		id, _, _, _ = readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerPong})
		id, _, _, _ = readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerError, remoteError: &RemoteError{Code: ErrorCodeExecution, TransactionState: TransactionStateNone, Message: "rolled back"}})
		id, _, _, _ = readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerTransactionStarted})
		id, _, _, _ = readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerTransactionRolledBack})
	}()
	tx, err := client.Begin(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), "SELECT FROM"); err == nil {
		t.Fatal("expected remote compile error")
	}
	if tx.terminal {
		t.Fatal("Active remote state terminated Tx")
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping in Tx: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "UPDATE users SET name = 'x'"); err == nil {
		t.Fatal("expected DML error")
	}
	if !tx.terminal || client.tx != nil {
		t.Fatal("None remote state did not clear Tx")
	}
	tx2, err := client.Begin(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteErrorDoesNotPoisonConnection(t *testing.T) {
	client, server := pipeClient()
	defer client.Close()
	defer server.Close()
	go func() {
		id, _, _, _ := readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerError, remoteError: &RemoteError{Code: ErrorCodeDatabase, TransactionState: TransactionStateNone, Message: "authorization denied"}})
		id, _, _, _ = readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerPong})
	}()
	_, err := client.Exec(context.Background(), "DELETE FROM users")
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != ErrorCodeDatabase {
		t.Fatalf("error = %#v", err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueryContextCancellationClosesConnection(t *testing.T) {
	client, server := pipeClient()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.Query(ctx, "SELECT id FROM users"); done <- err }()
	if _, _, _, err := readClientTestFrame(server); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled Query succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled Query did not unblock")
	}
	if !client.closed {
		t.Fatal("cancelled query did not close client")
	}
}

func TestSchemaFingerprintGateAndExtraVisibleTables(t *testing.T) {
	required := TableIdentity{TableID: 1, Fingerprint: SchemaFingerprint{1}}
	extra := TableIdentity{TableID: 2, Fingerprint: SchemaFingerprint{2}}
	cases := []struct {
		name   string
		tables []TableIdentity
		want   any
	}{
		{"equal with extra", []TableIdentity{required, extra}, nil},
		{"mismatch", []TableIdentity{{TableID: 1, Fingerprint: SchemaFingerprint{9}}}, new(SchemaMismatchError)},
		{"missing", []TableIdentity{extra}, new(SchemaUnavailableError)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, server := pipeClient()
			defer server.Close()
			go func() {
				id, _, _, err := readClientTestFrame(server)
				if err == nil {
					writeTestMessage(t, server, id, serverMessage{kind: kindServerHelloAck, protocolVersion: 1, maxFramePayload: maxFramePayload, capabilities: 7, tables: tc.tables})
				}
			}()
			err := client.handshake(context.Background(), 7, []TableIdentity{required})
			if tc.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				_ = client.Close()
				return
			}
			switch tc.want.(type) {
			case *SchemaMismatchError:
				var target *SchemaMismatchError
				if !errors.As(err, &target) {
					t.Fatalf("error = %T %v", err, err)
				}
			case *SchemaUnavailableError:
				var target *SchemaUnavailableError
				if !errors.As(err, &target) {
					t.Fatalf("error = %T %v", err, err)
				}
			}
		})
	}
}

func TestHelloAckValidationAndServerInfoCopy(t *testing.T) {
	client, server := pipeClient()
	defer server.Close()
	tables := []TableIdentity{{TableID: 1, Fingerprint: SchemaFingerprint{1}}}
	go func() {
		id, _, _, _ := readClientTestFrame(server)
		writeTestMessage(t, server, id, serverMessage{kind: kindServerHelloAck, protocolVersion: 1, maxFramePayload: maxFramePayload, capabilities: 7, tables: tables})
	}()
	if err := client.handshake(context.Background(), 7, tables); err != nil {
		t.Fatal(err)
	}
	info := client.ServerInfo()
	info.Tables[0].TableID = 99
	if client.ServerInfo().Tables[0].TableID != 1 {
		t.Fatal("ServerInfo exposed mutable backing storage")
	}
}

func TestCloseIsIdempotentAndRequestIDExhaustionIsChecked(t *testing.T) {
	client, server := pipeClient()
	_ = server.Close()
	client.nextRequestID = ^uint64(0)
	if _, err := client.allocateRequestID(); err == nil {
		t.Fatal("request ID wrapped")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("Ping closed = %v", err)
	}
}
