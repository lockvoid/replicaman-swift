# Replicaman for Swift

Replicaman synchronizes application data through ordered events and snapshots.
This Swift client handles delivery, retries, cancellation, and recovery. Your
application supplies authentication, payload handling, and two callbacks that
save protocol state together with application data.

Requires Swift 6, iOS 17 or later, or macOS 14 or later. The package has no
external dependencies and works with your application's storage system. It
implements the [Replicaman v1 protocol](https://github.com/lockvoid/replicaman-server/blob/main/docs/PROTOCOL.md).

## Add the package

For a local checkout, add this dependency and library product to `Package.swift`:

```swift
 dependencies: [
     .package(path: "../replicaman-swift")
 ],
 targets: [
     .target(name: "YourApp", dependencies: [
         .product(name: "ReplicamanSync", package: "replicaman-swift")
     ])
 ]
```

In Xcode, add the package directory as a local package and link `ReplicamanSync`
to your app target.

## Connect one stream

Create a `Scope` from your application's bootstrap response and persist one
client UUID per installation's local store. A fresh `ClientState(scope:)` has
no confirmed base: its first read downloads a snapshot, even if the server's
position is zero.

The following example runs in an async context. `database` and `credentials`
are APIs supplied by your application:

```swift
import ReplicamanSync

let transport = try URLSessionTransport(endpoint: serverURL) {
    await credentials.currentHeaders()
}
let configuration = ClientConfiguration(
    load: { stream in try await database.loadReplicaState(stream: stream) },
    commit: { transition in try await database.commitReplication(transition) }
)
let client = try await SyncClient(
    stream: scope.stream,
    configuration: configuration,
    transport: transport
)

let reader = Task { await client.runReads() }
let sender = Task { await client.runSender() }
```

`load` returns the persisted state for that stream, or a fresh state for its
scope. `commit` compares the complete expected state, applies the application
change, and saves the next state in **one durable transaction**. Return `false`
on a race; an exception must roll back everything. The
[storage integration guide](docs/INTEGRATION.md) explains local edits, snapshot
deadlines, and recovery archives. Implement that contract before using the
client with production data.

The transport appends `/replicaman/v1/{push,pull,snapshot}` to `serverURL`.
Use HTTPS in production; loopback HTTP is available for local development.
Credentials refresh for each request, and redirects are refused.

## Drive the client

Keep the tasks in your application's lifecycle. Cancel `reader` and `sender`
when unbinding the client. A read-only consumer needs only the reader. Create
one client per stream and run at most four concurrently for the default catalog
profile.

Call `readStep()` on launch, foreground entry, connectivity restoration, or a
notification hint. It coalesces with an active read; `runReads()` also polls.
Temporary failures retry automatically and preserve server retry deadlines.
Use `resume(.read)` or `resume(.send)` after repairing an access, format, or
storage problem.

A new client claims a persisted ownership token, so late replies to a replaced
client cannot change current state. Changing accounts or epochs uses
`replaceScope` and archives the old work. A different stream requires another
client.

## Limits and verification

The default snapshot profile allows 2,048 items and 64 MiB of decoded payload.
HTTP bodies are limited to 48 MiB after decompression, and network exchanges to
60 seconds. Payload formats and merge rules belong to the application; measure
staging and decoding memory on supported devices before increasing limits.

```sh
swift test --disable-swift-testing
xcrun swift-format lint --recursive Sources Tests Package.swift
```

Tests cover wire/hash fixtures, lost replies, cancellation, canonical events,
snapshot recovery, retries across restarts, callback rollback, and replaced
clients. The test store is a fixture rather than a production database adapter.
See [the integration guide](docs/INTEGRATION.md) for the complete storage
obligations and verification scope.

## License

[MIT](LICENSE).
