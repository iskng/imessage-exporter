# Database Integration Notes

## Unix Socket Export Overview

The `lib_db` crate now focuses solely on exporting message data over a local
Unix domain socket. The socket protocol is intentionally minimal:

- `I<len><payload>` inserts a batch of JSON encoded messages
- `F` instructs the receiver to flush any buffered state
- Responses are a single byte: `K` (success) or `E` (error)

All timestamps are serialized using `chrono`'s built-in `serde` support, so
they appear as RFC 3339 strings (e.g. `"2024-01-01T00:00:00Z"`). Optional
fields are represented with `Option<T>` in the `Message` struct so the exporter
can faithfully forward whatever data is present in the iMessage database.

### Integration Checklist

1. Point `DBPATH` at the Unix socket path your consumer listens on
2. Read framed requests following the protocol above
3. Decode the JSON payload into `Vec<Message>` (or your own struct with
   matching fields)
4. Return the single-byte status before handling the next command

This approach keeps the exporter transport-agnostic while avoiding the
complexity of maintaining additional database clients.

### Maintenance Notes

- Keep dependencies updated, especially `tokio`, `serde`, and `chrono`
- Regularly test datetime serialization to ensure downstream compatibility
