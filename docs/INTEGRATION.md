# Integrating Replicaman with application storage

The SDK manages protocol state; your application owns its data and durable transactions. This guide describes atomic commits, optimistic edits, and snapshot recovery. Start with the [README](../README.md) for installation and a connection example.

## One application transaction

`ClientConfiguration` contains exactly two `@Sendable` callbacks:

```swift
load: (String) async throws -> ClientState
commit: (Transition) async throws -> Bool
```

`load(stream)` reads the application's metadata for that stream. Return a fresh
`ClientState(scope:)` when it has no saved state. Every load must return that same
stream in `state.scope.stream`; the SDK rejects mismatches before using the state.
The application binds these callbacks to its local account/store generation and
rejects access after that context closes or changes.

`commit` must do all of the following in **one transaction**:

1. Select `transition.expected.scope.stream` in the bound local store and compare
   the full stored state with `transition.expected`; return `false` on a race.
2. Apply `transition.change` to the confirmed data, intents, and recovery archive.
3. Save `transition.next`, including the queue, cursor, and recovery barrier.

An exception must roll back everything. Never acknowledge a page after saving
only its cursor. If optimistic work cannot be reconciled, archive it in the same
transaction instead of blocking confirmed reading. For snapshots, validate the
assembled image, replace the confirmed base, and reconcile **current** local
intents, including edits made during download. Page order does not promise parent
records before children.

Immediately before committing a snapshot, compare `validUntilUptime` with
`ProcessInfo.processInfo.systemUptime`. If the deadline has elapsed, roll back and
return `false`; the SDK starts a new attempt. The deadline is local and ephemeral.

Every local edit saves its intent and `state.edited()` atomically. Encode an
outbound operation against that revision, then call
`prepare(message, expectedRevision:)`. A `false` result means recompute against
the latest state. The SDK permits one prepared operation per stream and blocks
preparation during snapshots. The original ID, hash, and bytes survive retries.

An accepted push does not advance the read cursor. The application applies its
own canonical event too; `Settlement.decided` removes the corresponding
optimistic edit. A server noop has a reason and no payload; an empty payload is
still ordinary application data. `Settlement.disputed` requires preserving the
original work for recovery.

## Recovery

Snapshots stage in bounded memory, separately from the confirmed base. The
attempt, pending list, effective page limit, and preparation barrier persist.
Restart retries that creation request; abandoned attempts receive new IDs.
Defaults bound an image to 2,048 items and 64 MiB of decoded payload. Transport
bodies are bounded to 48 MiB after decompression and exchanges to 60 seconds.
JSON and application decoding consume additional memory; measure peak usage on
supported devices before increasing these limits.

Disconnects, 408, 429, and 5xx retry automatically. Eight failures are an
observable status, not a pause. Read and send counters remain independent.
Server retry deadlines persist across restarts. While running, delays use a
monotonic clock; restoration uses the device's wall clock to calculate the
remaining delay. Changing that clock across a restart can affect the wait.

`resume(.read)` or `resume(.send)` follows repair of an access, format, or storage
problem. Neither replaces an operation ID nor bypasses a server retry deadline.
`cancel()` retains the original operation/hash; cancellation may return a prior
acceptance, which still needs its canonical result. One inapplicable-page
failure triggers an archived snapshot recovery; a repeated failure is exposed.

`epoch_changed`, `stream_not_found`, and `stream_gone` are surfaced to the host,
which refreshes discovery and archives or closes the affected scope. Application
payload versions and merge rules remain the host's responsibility.

## Verification scope

Tests share the Rust wire/hash fixture and exercise response loss, cancellation,
canonical own-event application, snapshot cuts, durable retry/attempt state,
callback rollback, shared-configuration stream isolation, and replaced-client
fencing. The test store uses an atomic file replacement for restart traces; it
does not establish the correctness of a production database transaction or
application codec. Those need their own tests.

URI syntax is delegated to Foundation's strict RFC 3986 initializer. A small
Network-framework check covers Foundation's acceptance of malformed bracketed
IP literals. The JSON reader exists because ordinary dictionary decoding loses
duplicate keys before they can be rejected.
