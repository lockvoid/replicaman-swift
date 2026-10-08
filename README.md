# Replicaman Swift client

A Swift 6 package for the [Replicaman v1 protocol](../replicaman-server/docs/PROTOCOL.md).
Requires iOS 17 or macOS 14. The application owns its database, models,
authorization, discovery, and conflict handling. The SDK has no storage framework
dependency and generates no application code.

## Connect

Create a `Scope` from the application's bootstrap response. Persist one `client`
UUID per local store. A fresh `ClientState(scope:)` has no base: the first read
downloads a snapshot, even if the server's position is zero.

```swift
let transport = try URLSessionTransport(endpoint: serverURL) {
    await credentials.currentHeaders()
}
let client = try await SyncClient(store: applicationStore, transport: transport)

let reader = Task { await client.runReads() }
let sender = Task { await client.runSender() }
```

Here `applicationStore` implements `SyncStore`; `credentials` is the application's
credential provider. Own these tasks in the application lifecycle and call
`reader.cancel()` / `sender.cancel()` on unbind. Call `readStep()` on launch, foreground, connectivity restoration,
or a doorbell hint; it coalesces with an active read. `runReads` also polls.
Catalog consumers need only the reader. Run at most four streams concurrently
for the initial catalog profile.

Creating a client atomically claims a local ownership token. A replaced instance
returns `Work.stopped`; it cannot adopt another instance's state or prepare more
work. `replaceScope` explicitly archives or isolates the old application data and
queue before binding a new account or epoch. The transport never follows
redirects, so request credentials do not travel to another origin.

## One application transaction

`SyncStore` has two methods:

```swift
func load() async throws -> ClientState
func commit(_ transition: Transition) async throws -> Bool
```

`commit` must do all of the following in **one transaction**:

1. Compare the stored state with `transition.expected`; return `false` on a race.
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

## Validate

```sh
swift test --disable-swift-testing
xcrun swift-format lint --recursive Sources Tests Package.swift
```

Tests share the Rust wire/hash fixture and exercise response loss, cancellation,
canonical own-event application, snapshot cuts, durable retry/attempt state,
callback rollback, and replaced-client fencing. The test store uses an atomic
file replacement for restart traces; it does not establish the correctness of a
production database adapter or application codec. Those need their own tests.

URI syntax is delegated to Foundation's strict RFC 3986 initializer. A small
Network-framework check covers Foundation's acceptance of malformed bracketed
IP literals. The JSON reader exists because ordinary dictionary decoding loses
duplicate keys before they can be rejected.
