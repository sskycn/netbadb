# NetbaDB diagnostics-only LSP

`netbadb-lsp` is a synchronous stdio language server for schema-driven SQL
diagnostics:

```sh
netbadb-lsp --schema path/to/schema.json
```

The schema argument is SDK Schema Spec v1, parsed once at startup by
`netbadb-schema-spec` and validated through Canonical Schema IR. Invalid JSON,
unknown fields or physical types, unsupported versions, and canonical schema
violations fail the process before the LSP initialization handshake. Schema
changes require restarting the language server; there is no file watcher or
hot reload.

## Protocol surface

The server uses LSP over stdin/stdout and keeps stdout exclusively for framed
JSON-RPC messages. It supports initialize, initialized, shutdown, exit, and
full-document `textDocument/didOpen`, `didChange`, and `didClose`
notifications. `didOpen` and accepted changes immediately publish diagnostics;
a valid document and `didClose` publish an empty array to clear stale errors.
Published diagnostics carry the current document version when one is known.

Only `TextDocumentSyncKind::FULL` is advertised. Incremental range edits are
ignored without changing document state, and stale document versions do not
replace newer buffers. The notification text is the source of truth: file URIs
are document identities and are never read from disk. Language IDs are not
restricted.

The server explicitly uses UTF-16 LSP positions. Compiler spans are half-open
UTF-8 byte ranges into the exact source buffer; the adapter validates their
bounds and character boundaries, then converts LF or CRLF line positions and
UTF-16 code-unit columns. Non-BMP characters therefore count as two UTF-16
units. A client that explicitly offers no UTF-16 position encoding is rejected
during initialization.

## Diagnostic semantics

One open document represents one NetbaDB SQL statement. `netbadb-tooling`
compiles that source against the loaded canonical schema and reports stable
parse, name-resolution, type, NULL, DML, grouping, and aggregate diagnostic
codes. Every LSP diagnostic has severity `Error`, source `netbadb`, a stable
snake-case code, the existing human compiler message, and an exact range.

The current compiler is fail-fast, so the server publishes at most one compiler
diagnostic per document. It does not attempt semicolon splitting, parser error
recovery, or string-based guesses to fabricate additional errors.

## Deliberate boundaries

The language server does not open `.ndb` or WAL files, load deployment manifest
v4, run recovery, start or contact `netbadbd`, execute SQL, or invoke physical
planning. It does not spawn `netbadb inspect` or parse Inspection JSON v1.
Runtime inspection remains the separate Database → planner → inspection DTO
path.

No completion, hover, go-to-definition, references, rename, formatting,
semantic tokens, code actions, physical-plan request, remote validation, or MCP
server is implemented or advertised in Phase 6E1. Future MCP tooling will
consume `netbadb-tooling` diagnostics and, where runtime inspection is needed,
`netbadb-inspect` DTOs directly.
