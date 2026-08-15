package netbadb

import "fmt"

// TableID is a stable canonical table identity.
type TableID uint64

// ColumnID is a stable canonical column identity.
type ColumnID uint32

// SchemaFingerprint is the Rust-authoritative SHA-256 fingerprint of a
// canonical table schema. This package compares fingerprints but never creates
// them.
type SchemaFingerprint [32]byte

type TableIdentity struct {
	TableID     TableID
	Fingerprint SchemaFingerprint
}

// Nullable represents a generated typed application value that may be
// database NULL. Its zero value is NULL.
type Nullable[T any] struct {
	Value T
	Valid bool
}

// Some constructs a non-NULL generated application value.
func Some[T any](value T) Nullable[T] { return Nullable[T]{Value: value, Valid: true} }

// Get returns the value and whether it is non-NULL.
func (value Nullable[T]) Get() (T, bool) { return value.Value, value.Valid }

type PhysicalType uint8

const (
	PhysicalTypeBool   PhysicalType = 1
	PhysicalTypeInt64  PhysicalType = 2
	PhysicalTypeUInt64 PhysicalType = 3
	PhysicalTypeText   PhysicalType = 4
)

func (t PhysicalType) String() string {
	switch t {
	case PhysicalTypeBool:
		return "bool"
	case PhysicalTypeInt64:
		return "int64"
	case PhysicalTypeUInt64:
		return "uint64"
	case PhysicalTypeText:
		return "text"
	default:
		return fmt.Sprintf("PhysicalType(%d)", uint8(t))
	}
}

type SemanticType struct {
	Physical PhysicalType
	Name     string
	Named    bool
}

type ValueKind uint8

const (
	ValueKindNull   ValueKind = 0
	ValueKindBool   ValueKind = 1
	ValueKindInt64  ValueKind = 2
	ValueKindUInt64 ValueKind = 3
	ValueKindText   ValueKind = 4
)

// Value is an explicitly tagged database scalar. NULL is a distinct value,
// never a Go nil or a zero-value convention.
type Value struct {
	kind ValueKind
	b    bool
	i    int64
	u    uint64
	s    string
}

func Null() Value                      { return Value{kind: ValueKindNull} }
func BoolValue(v bool) Value           { return Value{kind: ValueKindBool, b: v} }
func Int64Value(v int64) Value         { return Value{kind: ValueKindInt64, i: v} }
func UInt64Value(v uint64) Value       { return Value{kind: ValueKindUInt64, u: v} }
func TextValue(v string) Value         { return Value{kind: ValueKindText, s: v} }
func (v Value) Kind() ValueKind        { return v.kind }
func (v Value) IsNull() bool           { return v.kind == ValueKindNull }
func (v Value) Bool() (bool, bool)     { return v.b, v.kind == ValueKindBool }
func (v Value) Int64() (int64, bool)   { return v.i, v.kind == ValueKindInt64 }
func (v Value) UInt64() (uint64, bool) { return v.u, v.kind == ValueKindUInt64 }
func (v Value) Text() (string, bool)   { return v.s, v.kind == ValueKindText }

func (v Value) physicalType() (PhysicalType, bool) {
	switch v.kind {
	case ValueKindNull:
		return 0, false
	case ValueKindBool:
		return PhysicalTypeBool, true
	case ValueKindInt64:
		return PhysicalTypeInt64, true
	case ValueKindUInt64:
		return PhysicalTypeUInt64, true
	case ValueKindText:
		return PhysicalTypeText, true
	default:
		return 0, false
	}
}

type ResultColumn struct {
	Name     string
	Type     SemanticType
	Nullable bool
}

type TransactionState uint8

const (
	TransactionStateNone             TransactionState = 0
	TransactionStateActive           TransactionState = 1
	TransactionStateRollbackRequired TransactionState = 2
	TransactionStateCommitPending    TransactionState = 3
	TransactionStateRollbackPending  TransactionState = 4
)

type ServerInfo struct {
	ProtocolVersion uint16
	MaxFramePayload uint32
	Capabilities    uint64
	Tables          []TableIdentity
}

const (
	CapabilityExplicitTransactions uint64 = 0x1
	CapabilityAnalyze              uint64 = 0x2
	CapabilityStreamedQueryResults uint64 = 0x4
)
