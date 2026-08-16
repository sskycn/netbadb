# NetbaDB Inspection JSON v2

Inspection JSON v2 is the explicit machine-readable contract emitted by the
offline `netbadb inspect ... --format json` CLI. It is not serde serialization
of Rust planner or inspection enums. The CLI converts stable inspection DTOs
into separate, exhaustively matched JSON view types; planner, storage, page,
WAL, manifest path, TLS, listener, and authorization state are never exposed.

Output is pretty-printed UTF-8 JSON followed by one newline. Objects are
currently emitted in the field order shown here and arrays preserve canonical
or planner-defined DTO order. Consumers must select object fields by name.
There are no timestamps, hostnames, PIDs, addresses, or other run-specific
values.

Removing a field, changing its meaning, changing an enum tag, or changing a
typed scalar shape requires a future inspection JSON version. Any future
machine-visible structural change must first be evaluated for a version bump;
v2 goldens must not be silently rewritten.

Version 2 extends the v1 plan vocabulary with a typed bounded range-index
operator. Existing v1 operator shapes are unchanged; consumers can distinguish
the contracts using the envelope version.

## Envelopes

Catalog output:

```json
{
  "format": "netbadb-inspection",
  "version": 2,
  "kind": "catalog",
  "catalog": { "tables": [] }
}
```

Statement output:

```json
{
  "format": "netbadb-inspection",
  "version": 2,
  "kind": "statement",
  "statement": {
    "kind": "query",
    "access": { "read_tables": [], "write_tables": [] },
    "result": { "kind": "query", "columns": [] },
    "plan": { "kind": "query", "root": {} }
  }
}
```

The statement `kind` is one of `query`, `insert`, `update`, or `delete`.
Access arrays contain numeric canonical `TableId` values in typed logical
first-occurrence order.

## Catalog

`catalog.tables` preserves Canonical Schema declaration order. Each table is:

```json
{
  "table_id": 1,
  "name": "users",
  "fingerprint": "0123456789abcdef...64 lowercase hex characters...",
  "columns": [],
  "indexes": [],
  "statistics": null
}
```

Columns preserve declaration order:

```json
{
  "column_id": 1,
  "name": "id",
  "data_type": { "physical": "uint64", "semantic_name": "UserId" },
  "nullable": false,
  "primary_key": true
}
```

Physical-only types use `"semantic_name": null`. Physical spellings are
exactly `bool`, `int64`, `uint64`, and `text`.

Indexes preserve zero-based persistent registration order:

```json
{
  "column_id": 1,
  "column_name": "id",
  "registration_order": 0,
  "statistics": {
    "distinct_non_null_keys": 8,
    "null_count": 0,
    "tree_height": 1
  }
}
```

Table statistics use `row_count` and `managed_page_count`. Missing table or
index statistics are `null`. All statistics are the last explicit `ANALYZE`
snapshot and may be stale; inspection never refreshes them.

## Result fields and source identity

Statement result variants are explicitly tagged:

```json
{ "kind": "affected_rows" }
```

or:

```json
{
  "kind": "query",
  "columns": [
    {
      "name": "id",
      "data_type": { "physical": "uint64", "semantic_name": "UserId" },
      "nullable": false,
      "source": {
        "binding_id": 0,
        "table_id": 1,
        "column_id": 1,
        "relation_name": "users",
        "name": "id"
      }
    }
  ]
}
```

Derived aggregate output has `"source": null`. Source and plan column
references retain `binding_id`, so self-join scans with the same `table_id`
remain distinct. A full plan column reference additionally contains
`data_type` and `nullable`.

## Statement plans

The statement plan is tagged by `kind`:

- `query`: `root`
- `insert`: `table_id`, `table_name`, `values`
- `update`: `table_id`, `input`, `assignments`
- `delete`: `table_id`, `input`

An assignment contains a full `column` reference and typed `value` expression.
The inspected statement is never executed.

Every plan node uses an `operator` tag:

- `seq_scan`: `binding_id`, `table_id`, `table_name`, `columns`
- `index_scan`: scan fields plus `index_column` and typed `key`
- `range_index_scan`: scan fields plus `index_column`, `lower_bound`, and
  `upper_bound`
- `nested_loop_join`: `kind`, `predicate`, `left`, `right`; join kind is
  `inner`
- `filter`: `predicate`, `input`
- `sort`: `keys`, `input`
- `project`: `columns`, `input`
- `aggregate`: `group_keys`, `outputs`, `input`
- `limit`: `limit`, `input`

No index handle, page/root identity, row locator, or estimated/rejected cost is
present. The node reports the operator selected by the real planner.

Range bounds preserve endpoint semantics and scalar identity:

```json
{
  "operator": "range_index_scan",
  "binding_id": 0,
  "table_id": 1,
  "table_name": "items",
  "columns": [],
  "index_column": {},
  "lower_bound": {
    "kind": "included",
    "value": { "kind": "int64", "value": 5000 }
  },
  "upper_bound": {
    "kind": "excluded",
    "value": { "kind": "int64", "value": 5100 }
  }
}
```

Each bound kind is `included`, `excluded`, or `unbounded`; an unbounded bound
has no `value` field. The current planner emits this operator only for costed,
two-sided Int64/UInt64 predicates, while the DTO deliberately represents all
three endpoint forms.

A sort key contains `column`, `direction`, and `null_order`. Direction is
`asc` or `desc`; NULL order is `first` or `last`.

Aggregate outputs use explicit tags:

```json
{ "kind": "group_key", "column": {} }
```

or:

```json
{
  "kind": "aggregate",
  "function": "count",
  "input": { "kind": "all" },
  "output": {}
}
```

Functions are `count`, `sum`, `min`, and `max`. Aggregate input is either
`{"kind":"all"}` or `{"kind":"column","column":{...}}`.

## Expressions

Every expression contains `kind`, `data_type`, and `nullable`, plus fields for
its tagged variant:

- `column`: `column`
- `literal`: `value`
- `binary`: `operator`, `left`, `right`
- `unary`: `operator`, `expression`
- `is_null`: `expression`, `negated`

Binary operators are `eq`, `not_eq`, `lt`, `lt_eq`, `gt`, `gt_eq`, `and`, and
`or`. The current unary operator is `not`.

## Typed scalar values

Scalar identity is structural, never human-rendered text:

```json
{ "kind": "null" }
{ "kind": "bool", "value": true }
{ "kind": "int64", "value": -1 }
{ "kind": "uint64", "value": 42 }
{ "kind": "text", "value": "Ada" }
```

The signed and unsigned 64-bit values are serialized directly as JSON integer
numbers, never through `f64` or strings.

## Operational boundary

JSON errors are not part of v2. Any usage, manifest, open/recovery, compile,
inspection, serialization, or close failure writes a text diagnostic to
stderr, returns a nonzero status, and leaves stdout empty. Output is emitted
only after successful database close.

The CLI requires offline exclusive ownership and uses normal startup recovery,
which may modify persistent state after a crash. Deployment authorization
protects Protocol sessions and does not filter a local process that already
has filesystem access. Future LSP and MCP adapters consume inspection DTOs
directly; this JSON contract is not an internal architecture bus.
