package netbadb

import (
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	// ProtocolMagic is the fixed Protocol v1 frame marker.
	ProtocolMagic = "NDBP"
	// ProtocolVersion is the only protocol version supported by this module.
	ProtocolVersion uint16 = 1
	// FrameHeaderSize is the fixed Protocol v1 header width.
	FrameHeaderSize = 24
	// MaxFramePayload is the Protocol v1 payload allocation bound.
	MaxFramePayload uint32 = 16 * 1024 * 1024
	// MaxCollectionItems bounds every repeated wire collection.
	MaxCollectionItems uint32 = 65_536

	protocolVersion   = ProtocolVersion
	frameHeaderSize   = FrameHeaderSize
	maxFramePayload   = MaxFramePayload
	maxCollectionSize = MaxCollectionItems

	kindClientHello    uint16 = 0x0001
	kindClientExecute  uint16 = 0x0002
	kindClientBegin    uint16 = 0x0003
	kindClientCommit   uint16 = 0x0004
	kindClientRollback uint16 = 0x0005
	kindClientAnalyze  uint16 = 0x0006
	kindClientPing     uint16 = 0x0007

	kindServerHelloAck              uint16 = 0x8001
	kindServerQueryStart            uint16 = 0x8002
	kindServerQueryRow              uint16 = 0x8003
	kindServerQueryEnd              uint16 = 0x8004
	kindServerAffectedRows          uint16 = 0x8005
	kindServerTransactionStarted    uint16 = 0x8006
	kindServerTransactionCommitted  uint16 = 0x8007
	kindServerTransactionRolledBack uint16 = 0x8008
	kindServerAnalyzeAck            uint16 = 0x8009
	kindServerPong                  uint16 = 0x800a
	kindServerError                 uint16 = 0x8fff
)

type clientMessage struct {
	kind    uint16
	sql     string
	tableID TableID
}

type serverMessage struct {
	kind            uint16
	protocolVersion uint16
	maxFramePayload uint32
	capabilities    uint64
	tables          []TableIdentity
	columns         []ResultColumn
	values          []Value
	count           uint64
	remoteError     *RemoteError
}

func encodeClientFrame(requestID uint64, message clientMessage) ([]byte, error) {
	if requestID == 0 {
		return nil, &ProtocolError{Reason: "request ID zero is reserved"}
	}
	var payload []byte
	switch message.kind {
	case kindClientHello, kindClientCommit, kindClientRollback, kindClientPing:
	case kindClientExecute:
		if !utf8.ValidString(message.sql) {
			return nil, &ProtocolError{Reason: "SQL is not valid UTF-8"}
		}
		if uint64(len(message.sql))+4 > uint64(maxFramePayload) {
			return nil, &ProtocolError{Reason: "frame payload exceeds 16 MiB"}
		}
		payload = make([]byte, 4+len(message.sql))
		binary.LittleEndian.PutUint32(payload, uint32(len(message.sql)))
		copy(payload[4:], message.sql)
	case kindClientBegin, kindClientAnalyze:
		payload = make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, uint64(message.tableID))
	default:
		return nil, &ProtocolError{Reason: fmt.Sprintf("invalid client message kind %#04x", message.kind)}
	}
	frame := make([]byte, frameHeaderSize+len(payload))
	copy(frame[0:4], ProtocolMagic)
	binary.LittleEndian.PutUint16(frame[4:6], protocolVersion)
	binary.LittleEndian.PutUint16(frame[6:8], message.kind)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(len(payload)))
	binary.LittleEndian.PutUint64(frame[16:24], requestID)
	copy(frame[24:], payload)
	return frame, nil
}

func readServerFrame(reader io.Reader) (uint64, serverMessage, error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		if err == io.ErrUnexpectedEOF {
			return 0, serverMessage{}, &ProtocolError{Reason: "truncated frame header"}
		}
		return 0, serverMessage{}, err
	}
	if string(header[0:4]) != ProtocolMagic {
		return 0, serverMessage{}, &ProtocolError{Reason: "invalid frame magic"}
	}
	if version := binary.LittleEndian.Uint16(header[4:6]); version != protocolVersion {
		return 0, serverMessage{}, &ProtocolError{Reason: fmt.Sprintf("unsupported frame version %d", version)}
	}
	kind := binary.LittleEndian.Uint16(header[6:8])
	if flags := binary.LittleEndian.Uint16(header[8:10]); flags != 0 {
		return 0, serverMessage{}, &ProtocolError{Reason: "nonzero frame flags"}
	}
	if reserved := binary.LittleEndian.Uint16(header[10:12]); reserved != 0 {
		return 0, serverMessage{}, &ProtocolError{Reason: "nonzero frame reserved field"}
	}
	length := binary.LittleEndian.Uint32(header[12:16])
	if length > maxFramePayload {
		return 0, serverMessage{}, &ProtocolError{Reason: "frame payload exceeds 16 MiB"}
	}
	requestID := binary.LittleEndian.Uint64(header[16:24])
	if requestID == 0 {
		return 0, serverMessage{}, &ProtocolError{Reason: "request ID zero is reserved"}
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, serverMessage{}, &ProtocolError{Reason: "truncated frame payload"}
		}
		return 0, serverMessage{}, err
	}
	message, err := decodeServerPayload(kind, payload)
	return requestID, message, err
}

type wireCursor struct {
	bytes []byte
	pos   int
}

func (c *wireCursor) remaining() int { return len(c.bytes) - c.pos }

func (c *wireCursor) take(n int) ([]byte, error) {
	if n < 0 || n > c.remaining() {
		return nil, &ProtocolError{Reason: "truncated payload"}
	}
	b := c.bytes[c.pos : c.pos+n]
	c.pos += n
	return b, nil
}

func (c *wireCursor) u8() (uint8, error) {
	b, err := c.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}
func (c *wireCursor) u16() (uint16, error) {
	b, err := c.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}
func (c *wireCursor) u32() (uint32, error) {
	b, err := c.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}
func (c *wireCursor) u64() (uint64, error) {
	b, err := c.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}
func (c *wireCursor) i64() (int64, error) {
	v, err := c.u64()
	return int64(v), err
}
func (c *wireCursor) boolean() (bool, error) {
	v, err := c.u8()
	if err != nil {
		return false, err
	}
	if v > 1 {
		return false, &ProtocolError{Reason: fmt.Sprintf("invalid boolean byte %d", v)}
	}
	return v == 1, nil
}
func (c *wireCursor) string() (string, error) {
	n, err := c.u32()
	if err != nil {
		return "", err
	}
	if uint64(n) > uint64(c.remaining()) {
		return "", &ProtocolError{Reason: "string length exceeds remaining payload"}
	}
	b, _ := c.take(int(n))
	if !utf8.Valid(b) {
		return "", &ProtocolError{Reason: "invalid UTF-8 string"}
	}
	return string(b), nil
}
func (c *wireCursor) count(minimum int) (int, error) {
	n, err := c.u32()
	if err != nil {
		return 0, err
	}
	if n > maxCollectionSize {
		return 0, &ProtocolError{Reason: "collection exceeds 65536 items"}
	}
	if minimum <= 0 || uint64(n)*uint64(minimum) > uint64(c.remaining()) {
		return 0, &ProtocolError{Reason: "collection count exceeds remaining payload"}
	}
	return int(n), nil
}
func (c *wireCursor) finish() error {
	if c.remaining() != 0 {
		return &ProtocolError{Reason: "payload has trailing bytes"}
	}
	return nil
}

func decodeServerPayload(kind uint16, payload []byte) (serverMessage, error) {
	c := wireCursor{bytes: payload}
	m := serverMessage{kind: kind}
	var err error
	switch kind {
	case kindServerHelloAck:
		if m.protocolVersion, err = c.u16(); err != nil {
			return m, err
		}
		reserved, e := c.u16()
		if e != nil {
			return m, e
		}
		if reserved != 0 {
			return m, &ProtocolError{Reason: "nonzero HelloAck reserved field"}
		}
		if m.maxFramePayload, err = c.u32(); err != nil {
			return m, err
		}
		if m.capabilities, err = c.u64(); err != nil {
			return m, err
		}
		count, e := c.count(40)
		if e != nil {
			return m, e
		}
		m.tables = make([]TableIdentity, 0, count)
		for i := 0; i < count; i++ {
			id, e := c.u64()
			if e != nil {
				return m, e
			}
			fp, e := c.take(32)
			if e != nil {
				return m, e
			}
			var fingerprint SchemaFingerprint
			copy(fingerprint[:], fp)
			m.tables = append(m.tables, TableIdentity{TableID: TableID(id), Fingerprint: fingerprint})
		}
	case kindServerQueryStart:
		count, e := c.count(9)
		if e != nil {
			return m, e
		}
		m.columns = make([]ResultColumn, 0, count)
		for i := 0; i < count; i++ {
			name, e := c.string()
			if e != nil {
				return m, e
			}
			semantic, e := decodeSemanticType(&c)
			if e != nil {
				return m, e
			}
			nullable, e := c.boolean()
			if e != nil {
				return m, e
			}
			m.columns = append(m.columns, ResultColumn{Name: name, Type: semantic, Nullable: nullable})
		}
	case kindServerQueryRow:
		count, e := c.count(1)
		if e != nil {
			return m, e
		}
		m.values = make([]Value, 0, count)
		for i := 0; i < count; i++ {
			value, e := decodeValue(&c)
			if e != nil {
				return m, e
			}
			m.values = append(m.values, value)
		}
	case kindServerQueryEnd, kindServerAffectedRows:
		if m.count, err = c.u64(); err != nil {
			return m, err
		}
	case kindServerTransactionStarted, kindServerTransactionCommitted, kindServerTransactionRolledBack, kindServerAnalyzeAck, kindServerPong:
	case kindServerError:
		code, e := c.u16()
		if e != nil {
			return m, e
		}
		if code < uint16(ErrorCodeProtocol) || code > uint16(ErrorCodeInternalResultMismatch) {
			return m, &ProtocolError{Reason: fmt.Sprintf("invalid error code %d", code)}
		}
		state, e := c.u8()
		if e != nil {
			return m, e
		}
		if state > uint8(TransactionStateRollbackPending) {
			return m, &ProtocolError{Reason: fmt.Sprintf("invalid transaction state %d", state)}
		}
		reserved, e := c.u8()
		if e != nil {
			return m, e
		}
		if reserved != 0 {
			return m, &ProtocolError{Reason: "nonzero Error reserved field"}
		}
		text, e := c.string()
		if e != nil {
			return m, e
		}
		m.remoteError = &RemoteError{Code: ErrorCode(code), TransactionState: TransactionState(state), Message: text}
	default:
		return m, &ProtocolError{Reason: fmt.Sprintf("invalid server message kind %#04x", kind)}
	}
	if err := c.finish(); err != nil {
		return m, err
	}
	return m, nil
}

func decodeSemanticType(c *wireCursor) (SemanticType, error) {
	physical, err := c.u8()
	if err != nil {
		return SemanticType{}, err
	}
	if physical < uint8(PhysicalTypeBool) || physical > uint8(PhysicalTypeText) {
		return SemanticType{}, &ProtocolError{Reason: fmt.Sprintf("invalid physical type %d", physical)}
	}
	named, err := c.boolean()
	if err != nil {
		return SemanticType{}, err
	}
	reserved, err := c.u16()
	if err != nil {
		return SemanticType{}, err
	}
	if reserved != 0 {
		return SemanticType{}, &ProtocolError{Reason: "nonzero SemanticType reserved field"}
	}
	result := SemanticType{Physical: PhysicalType(physical), Named: named}
	if named {
		result.Name, err = c.string()
	}
	return result, err
}

func decodeValue(c *wireCursor) (Value, error) {
	tag, err := c.u8()
	if err != nil {
		return Value{}, err
	}
	switch ValueKind(tag) {
	case ValueKindNull:
		return Null(), nil
	case ValueKindBool:
		v, e := c.boolean()
		return BoolValue(v), e
	case ValueKindInt64:
		v, e := c.i64()
		return Int64Value(v), e
	case ValueKindUInt64:
		v, e := c.u64()
		return UInt64Value(v), e
	case ValueKindText:
		v, e := c.string()
		return TextValue(v), e
	default:
		return Value{}, &ProtocolError{Reason: fmt.Sprintf("invalid scalar value tag %d", tag)}
	}
}
