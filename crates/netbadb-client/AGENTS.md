# Synchronous remote-client rules

- This crate is synchronous and blocking only. Do not introduce Tokio, async
  APIs, pooling, request pipelining, multiplexing, reconnect, or replay.
- Use `netbadb-protocol` for every Protocol v1 frame and domain value. Do not
  duplicate frame, scalar, semantic-type, transaction-state, or error codecs.
- A malformed response, wrong request ID, invalid row, unexpected response
  sequence, or premature EOF MUST poison and close the connection. A valid
  `ServerMessage::Error` is an application response and MUST NOT poison it.
- Unfinished `Rows` MUST NOT be silently dropped while preserving connection
  reuse. Explicit `Rows::close` may drain the response; `Drop` closes it.
- An active transaction MUST NOT be silently abandoned while preserving
  connection reuse. Explicit rollback confirms cleanup; `Drop` closes it and
  relies on the server's disconnect lifecycle.
- `Drop` MUST NOT perform hidden fallible commit or rollback network
  operations. Lost responses retain ambiguous-outcome semantics.
