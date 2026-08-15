package netbadb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
)

func testHeader(kind uint16, payloadLength uint32, requestID uint64) []byte {
	b := make([]byte, frameHeaderSize)
	copy(b, "NDBP")
	binary.LittleEndian.PutUint16(b[4:6], 1)
	binary.LittleEndian.PutUint16(b[6:8], kind)
	binary.LittleEndian.PutUint32(b[12:16], payloadLength)
	binary.LittleEndian.PutUint64(b[16:24], requestID)
	return b
}

func TestClientGoldenFramesMatchRust(t *testing.T) {
	hello, err := encodeClientFrame(1, clientMessage{kind: kindClientHello})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hello, testHeader(0x0001, 0, 1)) {
		t.Fatalf("Hello golden mismatch: %x", hello)
	}

	begin, err := encodeClientFrame(7, clientMessage{kind: kindClientBegin, tableID: 42})
	if err != nil {
		t.Fatal(err)
	}
	wantBegin := append(testHeader(0x0003, 8, 7), []byte{42, 0, 0, 0, 0, 0, 0, 0}...)
	if !bytes.Equal(begin, wantBegin) {
		t.Fatalf("Begin golden mismatch: %x", begin)
	}

	sql := "SELECT id FROM users"
	execute, err := encodeClientFrame(9, clientMessage{kind: kindClientExecute, sql: sql})
	if err != nil {
		t.Fatal(err)
	}
	wantExecute := testHeader(0x0002, uint32(4+len(sql)), 9)
	wantExecute = append(wantExecute, byte(len(sql)), 0, 0, 0)
	wantExecute = append(wantExecute, sql...)
	if !bytes.Equal(execute, wantExecute) {
		t.Fatalf("Execute golden mismatch: %x", execute)
	}
}

func TestServerGoldenFramesMatchRust(t *testing.T) {
	fingerprint := SchemaFingerprint{}
	for i := range fingerprint {
		fingerprint[i] = 0xab
	}
	cases := []struct {
		name    string
		id      uint64
		message serverMessage
		want    []byte
	}{
		{"HelloAck", 1, serverMessage{kind: kindServerHelloAck, protocolVersion: 1, maxFramePayload: maxFramePayload, capabilities: 7, tables: []TableIdentity{{TableID: 5, Fingerprint: fingerprint}}}, nil},
		{"AffectedRows", 2, serverMessage{kind: kindServerAffectedRows, count: 3}, nil},
		{"QueryStart", 3, serverMessage{kind: kindServerQueryStart, columns: []ResultColumn{{Name: "id", Type: SemanticType{Physical: PhysicalTypeUInt64, Name: "UserId", Named: true}}}}, nil},
		{"QueryRow", 4, serverMessage{kind: kindServerQueryRow, values: []Value{Null(), BoolValue(true), Int64Value(-2), UInt64Value(3), TextValue("x")}}, nil},
		{"Error", 5, serverMessage{kind: kindServerError, remoteError: &RemoteError{Code: ErrorCodeCompile, TransactionState: TransactionStateActive, Message: "bad SQL"}}, nil},
	}
	cases[0].want = testHeader(0x8001, 60, 1)
	cases[0].want = append(cases[0].want, 1, 0, 0, 0, 0, 0, 0, 1, 7, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0)
	cases[0].want = append(cases[0].want, fingerprint[:]...)
	cases[1].want = append(testHeader(0x8005, 8, 2), 3, 0, 0, 0, 0, 0, 0, 0)
	cases[2].want = append(testHeader(0x8002, 25, 3), 1, 0, 0, 0, 2, 0, 0, 0, 'i', 'd', 3, 1, 0, 0, 6, 0, 0, 0, 'U', 's', 'e', 'r', 'I', 'd', 0)
	rowPayload := []byte{5, 0, 0, 0, 0, 1, 1, 2}
	rowPayload = append(rowPayload, []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...)
	rowPayload = append(rowPayload, 3, 3, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 'x')
	cases[3].want = append(testHeader(0x8003, uint32(len(rowPayload)), 4), rowPayload...)
	cases[4].want = append(testHeader(0x8fff, 15, 5), 7, 0, 1, 0, 7, 0, 0, 0, 'b', 'a', 'd', ' ', 'S', 'Q', 'L')

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeServerTestFrame(tc.id, tc.message)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("golden mismatch\n got %x\nwant %x", got, tc.want)
			}
			actualID, decoded, err := readServerFrame(bytes.NewReader(tc.want))
			if err != nil {
				t.Fatal(err)
			}
			if actualID != tc.id || !reflect.DeepEqual(decoded, tc.message) {
				t.Fatalf("roundtrip = %#v, %d", decoded, actualID)
			}
		})
	}
}

func TestAllClientKindsHaveExplicitTags(t *testing.T) {
	cases := []clientMessage{{kind: kindClientHello}, {kind: kindClientExecute}, {kind: kindClientBegin}, {kind: kindClientCommit}, {kind: kindClientRollback}, {kind: kindClientAnalyze}, {kind: kindClientPing}}
	for i, message := range cases {
		frame, err := encodeClientFrame(1, message)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := binary.LittleEndian.Uint16(frame[6:8]), uint16(i+1); got != want {
			t.Fatalf("tag = %#x, want %#x", got, want)
		}
	}
}

func TestAllServerMessagesDecodeInTheirOwnDirection(t *testing.T) {
	messages := []serverMessage{
		{kind: kindServerHelloAck, protocolVersion: 1, maxFramePayload: maxFramePayload, capabilities: 7, tables: []TableIdentity{}},
		{kind: kindServerQueryStart, columns: []ResultColumn{}},
		{kind: kindServerQueryRow, values: []Value{}},
		{kind: kindServerQueryEnd, count: 0},
		{kind: kindServerAffectedRows, count: 2},
		{kind: kindServerTransactionStarted},
		{kind: kindServerTransactionCommitted},
		{kind: kindServerTransactionRolledBack},
		{kind: kindServerAnalyzeAck},
		{kind: kindServerPong},
		{kind: kindServerError, remoteError: &RemoteError{Code: ErrorCodeStorage, TransactionState: TransactionStateRollbackPending, Message: "retry"}},
	}
	for index, message := range messages {
		frame, err := encodeServerTestFrame(uint64(index+1), message)
		if err != nil {
			t.Fatal(err)
		}
		requestID, decoded, err := readServerFrame(bytes.NewReader(frame))
		if err != nil {
			t.Fatal(err)
		}
		if requestID != uint64(index+1) || !reflect.DeepEqual(decoded, message) {
			t.Fatalf("decoded %#v for request %d", decoded, requestID)
		}
	}
}

func TestMalformedServerFramesAreRejected(t *testing.T) {
	valid, err := encodeServerTestFrame(1, serverMessage{kind: kindServerPong})
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(offset int, values ...byte) []byte {
		b := append([]byte(nil), valid...)
		copy(b[offset:], values)
		return b
	}
	cases := map[string][]byte{
		"bad magic":        mutate(0, 'X'),
		"bad version":      mutate(4, 2, 0),
		"client-only kind": mutate(6, 1, 0),
		"flags":            mutate(8, 1, 0),
		"reserved":         mutate(10, 1, 0),
		"zero request":     mutate(16, 0, 0, 0, 0, 0, 0, 0, 0),
		"partial header":   valid[:10],
	}
	oversize := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(oversize[12:16], maxFramePayload+1)
	cases["oversize"] = oversize
	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := readServerFrame(bytes.NewReader(frame)); err == nil {
				t.Fatal("accepted malformed frame")
			}
		})
	}
}

func TestMalformedServerPayloadsAreRejectedWithoutLargeAllocations(t *testing.T) {
	frame := func(kind uint16, payload []byte) []byte {
		return append(testHeader(kind, uint32(len(payload)), 1), payload...)
	}
	cases := map[string][]byte{
		"invalid UTF-8":      frame(kindServerError, []byte{1, 0, 0, 0, 1, 0, 0, 0, 0xff}),
		"invalid type":       frame(kindServerQueryStart, []byte{1, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0}),
		"invalid scalar":     frame(kindServerQueryRow, []byte{1, 0, 0, 0, 99}),
		"invalid bool":       frame(kindServerQueryRow, []byte{1, 0, 0, 0, 1, 2}),
		"huge count":         frame(kindServerQueryRow, []byte{0xff, 0xff, 0xff, 0xff}),
		"extra bytes":        frame(kindServerPong, []byte{0}),
		"invalid state":      frame(kindServerError, []byte{1, 0, 9, 0, 0, 0, 0, 0}),
		"invalid error code": frame(kindServerError, []byte{99, 0, 0, 0, 0, 0, 0, 0}),
	}
	for name, bytes := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := readServerFrame(bytesReader(bytes))
			var protocol *ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

func encodeServerTestFrame(requestID uint64, message serverMessage) ([]byte, error) {
	var payload bytes.Buffer
	putString := func(value string) {
		_ = binary.Write(&payload, binary.LittleEndian, uint32(len(value)))
		payload.WriteString(value)
	}
	putSemantic := func(value SemanticType) {
		payload.WriteByte(byte(value.Physical))
		if value.Named {
			payload.WriteByte(1)
		} else {
			payload.WriteByte(0)
		}
		payload.Write([]byte{0, 0})
		if value.Named {
			putString(value.Name)
		}
	}
	putValue := func(value Value) {
		payload.WriteByte(byte(value.kind))
		switch value.kind {
		case ValueKindBool:
			if value.b {
				payload.WriteByte(1)
			} else {
				payload.WriteByte(0)
			}
		case ValueKindInt64:
			_ = binary.Write(&payload, binary.LittleEndian, value.i)
		case ValueKindUInt64:
			_ = binary.Write(&payload, binary.LittleEndian, value.u)
		case ValueKindText:
			putString(value.s)
		}
	}
	switch message.kind {
	case kindServerHelloAck:
		_ = binary.Write(&payload, binary.LittleEndian, message.protocolVersion)
		_ = binary.Write(&payload, binary.LittleEndian, uint16(0))
		_ = binary.Write(&payload, binary.LittleEndian, message.maxFramePayload)
		_ = binary.Write(&payload, binary.LittleEndian, message.capabilities)
		_ = binary.Write(&payload, binary.LittleEndian, uint32(len(message.tables)))
		for _, table := range message.tables {
			_ = binary.Write(&payload, binary.LittleEndian, uint64(table.TableID))
			payload.Write(table.Fingerprint[:])
		}
	case kindServerQueryStart:
		_ = binary.Write(&payload, binary.LittleEndian, uint32(len(message.columns)))
		for _, column := range message.columns {
			putString(column.Name)
			putSemantic(column.Type)
			if column.Nullable {
				payload.WriteByte(1)
			} else {
				payload.WriteByte(0)
			}
		}
	case kindServerQueryRow:
		_ = binary.Write(&payload, binary.LittleEndian, uint32(len(message.values)))
		for _, value := range message.values {
			putValue(value)
		}
	case kindServerQueryEnd, kindServerAffectedRows:
		_ = binary.Write(&payload, binary.LittleEndian, message.count)
	case kindServerError:
		_ = binary.Write(&payload, binary.LittleEndian, uint16(message.remoteError.Code))
		payload.WriteByte(byte(message.remoteError.TransactionState))
		payload.WriteByte(0)
		putString(message.remoteError.Message)
	case kindServerTransactionStarted, kindServerTransactionCommitted, kindServerTransactionRolledBack, kindServerAnalyzeAck, kindServerPong:
	default:
		return nil, errors.New("unsupported test message")
	}
	result := testHeader(message.kind, uint32(payload.Len()), requestID)
	return append(result, payload.Bytes()...), nil
}
