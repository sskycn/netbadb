package testschema

import (
	"errors"
	"testing"

	netbadb "github.com/sskycn/netbadb/sdk/go"
)

var _ Queryer = (*netbadb.Client)(nil)
var _ Queryer = (*netbadb.Tx)(nil)

func canonicalColumns() []netbadb.ResultColumn {
	return []netbadb.ResultColumn{
		{Name: "id", Type: netbadb.SemanticType{Physical: netbadb.PhysicalTypeInt64, Name: "UserId", Named: true}},
		{Name: "name", Type: netbadb.SemanticType{Physical: netbadb.PhysicalTypeText}, Nullable: true},
	}
}

func TestGeneratedIdentityAndFreshRequiredSchemas(t *testing.T) {
	identity := UsersIdentity()
	if identity.TableID != UsersTableId || identity.Fingerprint == (netbadb.SchemaFingerprint{}) {
		t.Fatalf("identity = %#v", identity)
	}
	if UsersIdColumnId != 1 || UsersNameColumnId != 2 {
		t.Fatalf("column IDs = %d, %d", UsersIdColumnId, UsersNameColumnId)
	}
	first := RequiredSchemas()
	first[0].Fingerprint[0] ^= 1
	if first[0] == RequiredSchemas()[0] {
		t.Fatal("RequiredSchemas returned shared mutable state")
	}
}

func TestNullableZeroAndSome(t *testing.T) {
	var null netbadb.Nullable[string]
	if _, valid := null.Get(); valid {
		t.Fatal("Nullable zero value is not NULL")
	}
	value, valid := netbadb.Some("Ada").Get()
	if value != "Ada" || !valid {
		t.Fatalf("Some.Get = %q, %t", value, valid)
	}
}

func TestDecodeUsersRowPreservesNominalAndNullableTypes(t *testing.T) {
	row, err := DecodeUsersRow([]netbadb.Value{netbadb.Int64Value(42), netbadb.Null()})
	if err != nil {
		t.Fatal(err)
	}
	if row.Id != UserId(42) || row.Name.Valid {
		t.Fatalf("row = %#v", row)
	}
	row, err = DecodeUsersRow([]netbadb.Value{netbadb.Int64Value(43), netbadb.TextValue("Ada")})
	if err != nil {
		t.Fatal(err)
	}
	if row.Name.Value != "Ada" || !row.Name.Valid {
		t.Fatalf("nullable name = %#v", row.Name)
	}
	if _, err := DecodeUsersRow([]netbadb.Value{netbadb.TextValue("42"), netbadb.Null()}); err == nil {
		t.Fatal("wrong physical value was accepted")
	}
	if _, err := DecodeUsersRow([]netbadb.Value{netbadb.Null(), netbadb.Null()}); err == nil {
		t.Fatal("NULL non-null value was accepted")
	}
}

func TestValidateUsersColumnsRequiresExactCanonicalShape(t *testing.T) {
	if err := ValidateUsersColumns(canonicalColumns()); err != nil {
		t.Fatal(err)
	}
	mutations := []func([]netbadb.ResultColumn){
		func(columns []netbadb.ResultColumn) { columns[0], columns[1] = columns[1], columns[0] },
		func(columns []netbadb.ResultColumn) { columns[0].Name = "user_id" },
		func(columns []netbadb.ResultColumn) { columns[0].Type.Physical = netbadb.PhysicalTypeUInt64 },
		func(columns []netbadb.ResultColumn) { columns[0].Type.Name = "TeamId" },
		func(columns []netbadb.ResultColumn) { columns[0].Nullable = true },
	}
	for index, mutate := range mutations {
		columns := canonicalColumns()
		mutate(columns)
		err := ValidateUsersColumns(columns)
		var shape *netbadb.ResultShapeError
		if !errors.As(err, &shape) {
			t.Fatalf("mutation %d error = %T %v", index, err, err)
		}
	}
}
