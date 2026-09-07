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
	PhysicalTypeBool    PhysicalType = 1
	PhysicalTypeInt64   PhysicalType = 2
	PhysicalTypeUInt64  PhysicalType = 3
	PhysicalTypeText    PhysicalType = 4
	PhysicalTypeInt8    PhysicalType = 5
	PhysicalTypeInt16   PhysicalType = 6
	PhysicalTypeInt32   PhysicalType = 7
	PhysicalTypeInt128  PhysicalType = 8
	PhysicalTypeUInt8   PhysicalType = 9
	PhysicalTypeUInt16  PhysicalType = 10
	PhysicalTypeUInt32  PhysicalType = 11
	PhysicalTypeUInt128 PhysicalType = 12
	PhysicalTypeFloat32 PhysicalType = 13
	PhysicalTypeFloat64 PhysicalType = 14
	PhysicalTypeBytes   PhysicalType = 15
)

func (t PhysicalType) String() string {
	switch t {
	case PhysicalTypeBool:
		return "bool"
	case PhysicalTypeInt8:
		return "int8"
	case PhysicalTypeInt16:
		return "int16"
	case PhysicalTypeInt32:
		return "int32"
	case PhysicalTypeInt64:
		return "int64"
	case PhysicalTypeInt128:
		return "int128"
	case PhysicalTypeUInt8:
		return "uint8"
	case PhysicalTypeUInt16:
		return "uint16"
	case PhysicalTypeUInt32:
		return "uint32"
	case PhysicalTypeUInt64:
		return "uint64"
	case PhysicalTypeUInt128:
		return "uint128"
	case PhysicalTypeFloat32:
		return "float32"
	case PhysicalTypeFloat64:
		return "float64"
	case PhysicalTypeText:
		return "text"
	case PhysicalTypeBytes:
		return "bytes"
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
	ValueKindNull    ValueKind = 0
	ValueKindBool    ValueKind = 1
	ValueKindInt64   ValueKind = 2
	ValueKindUInt64  ValueKind = 3
	ValueKindText    ValueKind = 4
	ValueKindInt8    ValueKind = 5
	ValueKindInt16   ValueKind = 6
	ValueKindInt32   ValueKind = 7
	ValueKindInt128  ValueKind = 8
	ValueKindUInt8   ValueKind = 9
	ValueKindUInt16  ValueKind = 10
	ValueKindUInt32  ValueKind = 11
	ValueKindUInt128 ValueKind = 12
	ValueKindFloat32 ValueKind = 13
	ValueKindFloat64 ValueKind = 14
	ValueKindBytes   ValueKind = 15
)

// Value is an explicitly tagged database scalar. NULL is a distinct value,
// never a Go nil or a zero-value convention.
type Value struct {
	kind ValueKind
	b    bool
	i    int64
	u    uint64
	f32  float32
	f64  float64
	s    string
	raw  string
}

func Null() Value                        { return Value{kind: ValueKindNull} }
func BoolValue(v bool) Value             { return Value{kind: ValueKindBool, b: v} }
func Int64Value(v int64) Value           { return Value{kind: ValueKindInt64, i: v} }
func Int8Value(v int8) Value             { return Value{kind: ValueKindInt8, i: int64(v)} }
func Int16Value(v int16) Value           { return Value{kind: ValueKindInt16, i: int64(v)} }
func Int32Value(v int32) Value           { return Value{kind: ValueKindInt32, i: int64(v)} }
func UInt8Value(v uint8) Value           { return Value{kind: ValueKindUInt8, u: uint64(v)} }
func UInt16Value(v uint16) Value         { return Value{kind: ValueKindUInt16, u: uint64(v)} }
func UInt32Value(v uint32) Value         { return Value{kind: ValueKindUInt32, u: uint64(v)} }
func UInt64Value(v uint64) Value         { return Value{kind: ValueKindUInt64, u: v} }
func Float32Value(v float32) Value       { return Value{kind: ValueKindFloat32, f32: v} }
func Float64Value(v float64) Value       { return Value{kind: ValueKindFloat64, f64: v} }
func TextValue(v string) Value           { return Value{kind: ValueKindText, s: v} }
func BytesValue(v []byte) Value          { return Value{kind: ValueKindBytes, raw: string(v)} }
func (v Value) Kind() ValueKind          { return v.kind }
func (v Value) IsNull() bool             { return v.kind == ValueKindNull }
func (v Value) Bool() (bool, bool)       { return v.b, v.kind == ValueKindBool }
func (v Value) Int64() (int64, bool)     { return v.i, v.kind == ValueKindInt64 }
func (v Value) Int8() (int8, bool)       { return int8(v.i), v.kind == ValueKindInt8 }
func (v Value) Int16() (int16, bool)     { return int16(v.i), v.kind == ValueKindInt16 }
func (v Value) Int32() (int32, bool)     { return int32(v.i), v.kind == ValueKindInt32 }
func (v Value) UInt8() (uint8, bool)     { return uint8(v.u), v.kind == ValueKindUInt8 }
func (v Value) UInt16() (uint16, bool)   { return uint16(v.u), v.kind == ValueKindUInt16 }
func (v Value) UInt32() (uint32, bool)   { return uint32(v.u), v.kind == ValueKindUInt32 }
func (v Value) UInt64() (uint64, bool)   { return v.u, v.kind == ValueKindUInt64 }
func (v Value) Float32() (float32, bool) { return v.f32, v.kind == ValueKindFloat32 }
func (v Value) Float64() (float64, bool) { return v.f64, v.kind == ValueKindFloat64 }
func (v Value) Text() (string, bool)     { return v.s, v.kind == ValueKindText }
func (v Value) Bytes() ([]byte, bool) {
	if v.kind != ValueKindBytes {
		return nil, false
	}
	return []byte(v.raw), true
}

func (v Value) physicalType() (PhysicalType, bool) {
	switch v.kind {
	case ValueKindNull:
		return 0, false
	case ValueKindBool:
		return PhysicalTypeBool, true
	case ValueKindInt8:
		return PhysicalTypeInt8, true
	case ValueKindInt16:
		return PhysicalTypeInt16, true
	case ValueKindInt32:
		return PhysicalTypeInt32, true
	case ValueKindInt64:
		return PhysicalTypeInt64, true
	case ValueKindUInt8:
		return PhysicalTypeUInt8, true
	case ValueKindUInt16:
		return PhysicalTypeUInt16, true
	case ValueKindUInt32:
		return PhysicalTypeUInt32, true
	case ValueKindUInt64:
		return PhysicalTypeUInt64, true
	case ValueKindFloat32:
		return PhysicalTypeFloat32, true
	case ValueKindFloat64:
		return PhysicalTypeFloat64, true
	case ValueKindText:
		return PhysicalTypeText, true
	case ValueKindBytes:
		return PhysicalTypeBytes, true
	case ValueKindInt128, ValueKindUInt128:
		return 0, false
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
