# Go SDK direction

Go is a supported application language, not a dependency of the Rust database
core. The intended integration is:

```text
Go schema frontend / generated SDK
              ↓
      Canonical Schema IR
              ↓
        NetbaDB protocol
              ↓
            netbadbd
```

The starting repository contained no Go implementation or server protocol, so
this directory deliberately contains contract notes rather than a fake client.
The wire format should be versioned and specified first; then the Go client and
schema/query generator can be added with integration tests against `netbadbd`.

The future client must preserve semantic IDs, nullability, parameter types, and
result shapes. It should not make JSON maps the database execution IR.
