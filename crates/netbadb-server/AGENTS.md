# Server session rules

- The server depends on `netbadb-core` and the language-neutral protocol; core
  and lower layers MUST NOT depend on the server.
- `SessionState` owns the protocol transaction lifecycle. Failed commit or
  rollback attempts MUST retain retryable transaction handles, and disconnect
  handling MUST explicitly resolve or report an active transaction.
- Session and database execution remain synchronous. Future async networking
  MUST stay outside the synchronous database core.
- The dedicated database worker thread MUST construct and exclusively own the
  `Database`, every `SessionState`, and every transaction handle. Connection
  threads own sockets and exchange only typed Send-safe commands and responses.
- TCP listeners MUST remain loopback-only until authentication and transport
  security exist. A malformed frame is connection-fatal; do not guess a request
  identity or attempt to resynchronize the stream.
- Disconnect and shutdown cleanup MUST join connection and worker threads. A
  failed session rollback is fatal to the worker and MUST NOT be discarded so
  the process can continue serving requests.
- Connection admission, socket timeouts, and runtime metrics belong at the TCP
  runtime boundary. The connection-vector length is the admission-limit truth;
  metrics MUST NOT decide correctness.
- Result-row policy belongs in transport-neutral `SessionState`, but is checked
  only after core execution materializes the complete `QueryResult`. Do not
  describe it as an executor memory limit or statement timeout.
- Wire responses MUST expose stable protocol domain values and errors, never
  internal Rust layouts, discriminants, debug strings, pages, or row locators.
