import Foundation
import XCTest

@testable import ReplicamanSync

@MainActor
final class ClientTests: XCTestCase {
    func testFreshCursorZeroStillRequiresSnapshotAndCanonicalData() async throws {
        let scope = try testScope()
        let store = TestStore(ClientState(scope: try testScope()))
        let network = ScriptedTransport()
        await network.enqueue { method, request in
            XCTAssertEqual(method, "snapshot")
            XCTAssertEqual(request["page"], .null)
            return ok(
                snapshot(
                    scope: scope, attempt: try request.string("attempt"), cursor: 0,
                    items: [try message("initial baseline")]))
        }
        let client = try await SyncClient(store: store, transport: network)
        let work = await client.readStep()
        let state = try await client.state()
        let values = await store.values()
        XCTAssertEqual(work, .progress)
        XCTAssertEqual(state.cursor, 0)
        XCTAssertNil(state.recovery)
        XCTAssertEqual(values, ["initial baseline"])
    }

    func testFixedTargetNumericGapsAndEmptyFinalInterval() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        await network.enqueue { _, request in
            XCTAssertNil(request["to"])
            return ok(
                pull(
                    scope: scope, cursor: 2, to: 5, more: true,
                    events: [log(try message("two"), cursor: 2)]))
        }
        await network.enqueue { _, request in
            XCTAssertEqual(request["to"], .string("5"))
            XCTAssertEqual(request["cursor"], .string("2"))
            return ok(pull(scope: scope, cursor: 5, to: 5, head: 10))
        }
        let client = try await SyncClient(store: store, transport: network)
        let first = await client.readStep()
        let second = await client.readStep()
        let state = try await client.state()
        XCTAssertEqual(first, .progress)
        XCTAssertEqual(second, .idle)
        XCTAssertEqual(state.cursor, 5)
        XCTAssertEqual(state.head, 10)
    }

    func testLostPushResponseIsConfirmedByOwnCanonicalEventWithoutAnotherPush() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let clock = TestClock()
        let client = try await SyncClient(
            store: store, transport: network, now: clock.now, jitter: { 1 })
        let operation = try message(" ab")
        await store.edit(" ab")
        let revision = try await client.state().revision
        let prepared = try await client.prepare(operation, expectedRevision: revision)
        XCTAssertTrue(prepared)
        await network.enqueue { _, _ in throw SyncError.transient(retryAfter: nil) }
        let sending = await client.sendStep()
        XCTAssertEqual(sending, .retry(after: 1, failures: 1))
        await network.enqueue { method, _ in
            XCTAssertEqual(method, "pull")
            return ok(
                pull(
                    scope: scope, cursor: 1,
                    events: [
                        log(
                            try message("ab"), cursor: 1,
                            pending: Pending(op: operation.id, hash: operation.hash))
                    ]))
        }
        let reading = await client.readStep()
        let state = try await client.state()
        let data = await store.values()
        XCTAssertEqual(reading, .idle)
        XCTAssertNil(state.prepared)
        XCTAssertEqual(data, ["ab"])
        let intents = await store.intents()
        XCTAssertEqual(intents, [])
        clock.advance(1)
        let retried = await client.sendStep()
        let count = await network.count()
        XCTAssertEqual(retried, .idle)
        XCTAssertEqual(count, 2)
    }

    func testAcceptedReceiptNeverMovesCursorOrAllowsDependentPreparation() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let client = try await SyncClient(store: store, transport: network)
        let operation = try message(" ab")
        _ = try await client.prepare(operation, expectedRevision: client.state().revision)
        await network.enqueue { _, _ in
            ok(
                .object(
                    scope.fields.merging(verdict(op: operation.id, cursor: 2)) { _, rhs in rhs }))
        }
        _ = await client.sendStep()
        var state = try await client.state()
        XCTAssertEqual(state.cursor, 0)
        XCTAssertNotNil(state.prepared)
        do {
            _ = try await client.prepare(message("b"), expectedRevision: state.revision)
            XCTFail("prepared dependent operation before canonical result")
        } catch { XCTAssertEqual(error as? SyncError, .busy) }
        await network.enqueue { _, _ in
            ok(
                pull(
                    scope: scope, cursor: 2,
                    events: [
                        log(try message("other"), cursor: 1),
                        log(
                            try message("ab"), cursor: 2,
                            pending: Pending(op: operation.id, hash: operation.hash)),
                    ]))
        }
        _ = await client.readStep()
        state = try await client.state()
        let values = await store.values()
        XCTAssertEqual(values, ["other", "ab"])
        XCTAssertNil(state.prepared)
        let next = try await client.prepare(message("a!b"), expectedRevision: state.revision)
        XCTAssertTrue(next)
    }

    func testCancellationKeepsOriginalIdentityAcrossLossAndCreatesKnownRejection() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let clock = TestClock()
        let client = try await SyncClient(
            store: store, transport: network, now: clock.now, jitter: { 1 })
        let operation = try message("poison")
        _ = try await client.prepare(operation, expectedRevision: client.state().revision)
        try await client.cancel()
        await network.enqueue { _, request in
            XCTAssertNil(request["message"])
            XCTAssertEqual(request["cancel"], Pending(op: operation.id, hash: operation.hash).json)
            throw SyncError.transient(retryAfter: nil)
        }
        _ = await client.sendStep()
        clock.advance(1)
        await network.enqueue { _, request in
            XCTAssertEqual(request["cancel"], Pending(op: operation.id, hash: operation.hash).json)
            return ok(
                .object(
                    scope.fields.merging(
                        verdict(op: operation.id, status: "rejected", reason: "cancelled")
                    ) { _, rhs in rhs }))
        }
        _ = await client.sendStep()
        let state = try await client.state()
        XCTAssertNil(state.prepared)
        XCTAssertEqual(state.cursor, 0)
        XCTAssertEqual(state.lastReceipt?.outcome, .rejected(reason: "cancelled"))
    }

    func testEightFailuresDoNotPauseAndLongRetryAfterIsRespected() async throws {
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let clock = TestClock()
        let client = try await SyncClient(
            store: store, transport: network, now: clock.now, jitter: { 1 })
        _ = try await client.prepare(message("edit"), expectedRevision: client.state().revision)
        for number in 1...9 {
            await network.enqueue { _, _ in
                HTTPReply(status: 503, headers: ["Retry-After": "900"], body: Data())
            }
            let result = await client.sendStep()
            XCTAssertEqual(result, .retry(after: 900, failures: number))
            let before = await network.count()
            _ = await client.sendStep()
            let after = await network.count()
            XCTAssertEqual(before, after)
            clock.advance(900)
        }
        let state = try await client.state()
        XCTAssertNil(state.sendPause)
        XCTAssertNotNil(state.prepared)
        await network.enqueue { _, _ in
            ok(
                pull(
                    scope: try testScope(), cursor: 1,
                    events: [log(try message("read still works"), cursor: 1)]))
        }
        let read = await client.readStep()
        XCTAssertEqual(read, .idle)
    }

    func testPersistedSnapshotAttemptPendingAndLimitSurviveCrash() async throws {
        let file = FileManager.default.temporaryDirectory.appendingPathComponent(Wire.uuid())
        defer { try? FileManager.default.removeItem(at: file) }
        let scope = try testScope()
        let network = ScriptedTransport()
        let firstStore = TestStore(ClientState(scope: try testScope()), file: file)
        let clock = TestClock()
        var settings = ClientLimits()
        settings.pageItems = 1
        let first = try await SyncClient(
            store: firstStore, transport: network, limits: settings,
            now: clock.now, wallNow: clock.date, jitter: { 1 }
        )
        await network.enqueue { _, request in
            XCTAssertEqual(request["limit"], .number("1"))
            throw SyncError.transient(retryAfter: nil)
        }
        _ = await first.readStep()
        let before = try await first.state()
        XCTAssertNotNil(before.recovery)
        let reopened = try TestStore.open(file)
        settings.pageItems = 2
        clock.advance(1)
        let restarted = try await SyncClient(
            store: reopened, transport: network, limits: settings,
            now: clock.now, wallNow: clock.date
        )
        await network.enqueue { _, request in
            XCTAssertEqual(request["attempt"], .string(before.recovery!.attempt))
            XCTAssertEqual(request["limit"], .number("1"))
            XCTAssertEqual(request["pending"], .array([]))
            return ok(
                snapshot(
                    scope: scope, attempt: before.recovery!.attempt, cursor: 3,
                    items: [try message("restarted")]))
        }
        let result = await restarted.readStep()
        let state = try await restarted.state()
        XCTAssertEqual(result, .progress)
        XCTAssertEqual(state.cursor, 3)
        XCTAssertNil(state.recovery)
    }

    func testRetryAfterSurvivesRestartAndReadSuccessDoesNotResetSendFailures() async throws {
        let file = FileManager.default.temporaryDirectory.appendingPathComponent(Wire.uuid())
        defer { try? FileManager.default.removeItem(at: file) }

        let store = TestStore(baseState(), file: file)
        let network = ScriptedTransport()
        let clock = TestClock()
        let first = try await SyncClient(
            store: store, transport: network, now: clock.now, wallNow: clock.date, jitter: { 1 }
        )
        let operation = try message("outbound")
        _ = try await first.prepare(operation, expectedRevision: first.state().revision)
        await network.enqueue { _, _ in
            HTTPReply(status: 503, headers: ["Retry-After": "900"], body: Data())
        }
        await network.enqueue { _, _ in
            HTTPReply(status: 503, headers: ["Retry-After": "120"], body: Data())
        }
        _ = await first.sendStep()
        _ = await first.readStep()

        let reopened = try TestStore.open(file)
        let second = try await SyncClient(
            store: reopened, transport: network, now: clock.now, wallNow: clock.date
        )
        let sending = await second.sendStep()
        let reading = await second.readStep()
        let requests = await network.count()
        XCTAssertEqual(sending, .retry(after: 900, failures: 1))
        XCTAssertEqual(reading, .retry(after: 120, failures: 1))
        XCTAssertEqual(requests, 2)

        clock.advance(120)
        await network.enqueue { _, _ in ok(pull(scope: try testScope(), cursor: 0)) }
        let refreshed = await second.readStep()
        let state = try await second.state()
        XCTAssertEqual(refreshed, .idle)
        XCTAssertEqual(state.readRetry.failures, 0)
        XCTAssertEqual(state.sendRetry.failures, 1)
        XCTAssertNil(state.sendPause)

        let waiting = await second.sendStep()
        XCTAssertEqual(waiting, .retry(after: 780, failures: 1))
    }

    func testReplacedInstanceStopsBeforeItCanReadOrPrepareMoreWork() async throws {
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let first = try await SyncClient(store: store, transport: network)
        let oldRevision = try await first.state().revision
        await network.enqueue { _, _ in
            HTTPReply(status: 503, headers: ["Retry-After": "900"], body: Data())
        }
        _ = await first.readStep()
        let second = try await SyncClient(store: store, transport: network)

        let result = await first.readStep()
        let requests = await network.count()
        XCTAssertEqual(result, .stopped)
        XCTAssertEqual(requests, 1)

        do {
            _ = try await first.prepare(message("stale edit"), expectedRevision: oldRevision)
            XCTFail("The replaced client prepared work")
        } catch {
            XCTAssertEqual(error as? SyncError, .superseded)
        }
        let state = try await second.state()
        XCTAssertNil(state.prepared)
        XCTAssertNil(state.readPause)
    }

    func testReplacedInstanceDiscardsAnInFlightMalformedReply() async throws {
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let gate = ResponseGate()
        await network.enqueue { _, _ in
            await gate.hold()
            return HTTPReply(status: 200, body: Data("malformed".utf8))
        }
        let first = try await SyncClient(store: store, transport: network)
        let request = Task { await first.readStep() }
        await gate.waitUntilHeld()

        let second = try await SyncClient(store: store, transport: network)
        await gate.release()
        let result = await request.value
        let state = try await second.state()
        XCTAssertEqual(result, .stopped)
        XCTAssertEqual(state.cursor, 0)
        XCTAssertNil(state.readPause)
    }

    func testNewIntentDuringSnapshotIsPreservedAndCannotPrepare() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let client = try await SyncClient(store: store, transport: network)
        await network.enqueue { _, _ in fail("cursor_expired", 409) }
        _ = await client.readStep()
        await store.edit("created while downloading")
        let revision = try await client.state().revision
        do {
            _ = try await client.prepare(message("new"), expectedRevision: revision)
            XCTFail()
        } catch { XCTAssertEqual(error as? SyncError, .busy) }
        await network.enqueue { _, request in
            await store.edit("another newer intent")
            return ok(
                snapshot(
                    scope: scope, attempt: try request.string("attempt"), cursor: 4,
                    items: [try message("base")]))
        }
        _ = await client.readStep()
        let intents = await store.intents()
        let values = await store.values()
        XCTAssertEqual(intents, ["created while downloading", "another newer intent"])
        XCTAssertEqual(values, ["base"])
    }

    func testUnseenCoveragePreservesUnknownAndNewerReceiptAtBothSnapshotCuts() async throws {
        let scope = try testScope()
        let operation = try message("local")
        for position: Int64 in [4, 7] {
            var state = baseState()
            state.prepared = PreparedOperation(
                message: operation, hash: operation.hash,
                outcome: .accepted(cursor: 7, reason: nil), cancel: false)
            state.lastReceipt = Receipt(
                op: operation.id, hash: operation.hash, outcome: .accepted(cursor: 7, reason: nil))
            let op = OperationCoverage(pending: state.prepared!.pending, coverage: .unseen)
            let fields = verdict(op: operation.id, status: "unseen").merging([
                "hash": .string(operation.hash)
            ]) { _, rhs in rhs }
            let page = try SnapshotPage(
                data: snapshot(
                    scope: scope, attempt: scope.client, cursor: position, ops: [.object(fields)]
                ).encoded(), scope: scope, attempt: scope.client, pending: [op.pending], limit: 100)
            if position < 7 {
                let (next, settlements) = try StateChange.snapshot(page, in: state)
                XCTAssertEqual(next.prepared?.outcome, state.prepared?.outcome)
                XCTAssertTrue(settlements.isEmpty)
            } else {
                XCTAssertThrowsError(try StateChange.snapshot(page, in: state))
            }
        }
        var unknown = baseState()
        unknown.prepared = PreparedOperation(
            message: operation, hash: operation.hash, outcome: nil, cancel: false)
        let unseen = verdict(op: operation.id, status: "unseen").merging([
            "hash": .string(operation.hash)
        ]) { _, rhs in rhs }
        let page = try SnapshotPage(
            data: snapshot(scope: scope, attempt: scope.client, cursor: 4, ops: [.object(unseen)])
                .encoded(), scope: scope, attempt: scope.client,
            pending: [unknown.prepared!.pending], limit: 100)
        let (next, settled) = try StateChange.snapshot(page, in: unknown)
        XCTAssertEqual(next.prepared, unknown.prepared)
        XCTAssertTrue(settled.isEmpty)
    }

    func testSnapshotAcceptedBeforeAndAfterCutSettlesOnlyCoveredOperation() throws {
        let scope = try testScope()
        let operation = try message("local")
        for accepted: Int64 in [4, 7] {
            var state = baseState()
            state.prepared = PreparedOperation(
                message: operation, hash: operation.hash, outcome: nil, cancel: false)
            let fields = verdict(op: operation.id, cursor: accepted).merging([
                "hash": .string(operation.hash)
            ]) { _, rhs in rhs }
            let page = try SnapshotPage(
                data: snapshot(
                    scope: scope, attempt: scope.client, cursor: 5, ops: [.object(fields)]
                ).encoded(), scope: scope, attempt: scope.client,
                pending: [state.prepared!.pending], limit: 100)
            let (next, settlements) = try StateChange.snapshot(page, in: state)
            XCTAssertEqual(next.prepared == nil, accepted <= 5)
            XCTAssertEqual(settlements.count, accepted <= 5 ? 1 : 0)
        }
    }

    func testAtomicInstallFailureRetainsBaseAndResumeUsesCompletedImage() async throws {
        let scope = try testScope()
        let store = TestStore(ClientState(scope: try testScope()))
        let network = ScriptedTransport()
        let client = try await SyncClient(store: store, transport: network)
        await store.failNextInstall(.storage("disk_full"))
        await network.enqueue { _, request in
            ok(
                snapshot(
                    scope: scope, attempt: try request.string("attempt"), cursor: 2,
                    items: [try message("new")]))
        }
        let failed = await client.readStep()
        let before = try await client.state()
        let oldData = await store.values()
        XCTAssertEqual(failed, .paused("storage_error"))
        XCTAssertNil(before.cursor)
        XCTAssertEqual(oldData, [])
        try await client.resume(.read)
        let succeeded = await client.readStep()
        let after = try await client.state()
        let count = await network.count()
        XCTAssertEqual(succeeded, .progress)
        XCTAssertEqual(after.cursor, 2)
        XCTAssertEqual(count, 1)
    }

    func testOldAccountMalformedReplyIsDiscardedBeforeDecoding() async throws {
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let client = try await SyncClient(store: store, transport: network)
        await network.enqueue { _, _ in
            await store.replace(ClientState(scope: try testScope(subject: "user:2")))
            return HTTPReply(status: 200, body: Data("invalid JSON".utf8))
        }
        let work = await client.readStep()
        let state = await store.load()
        XCTAssertEqual(work, .stopped)
        XCTAssertEqual(state.scope.subject, "user:2")
        XCTAssertNil(state.readPause)
    }

    func testInapplicablePageGetsOneSnapshotThenVisiblePauseOnRepeatedFailure() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let client = try await SyncClient(store: store, transport: network)
        await store.failNextPage(.inapplicable)
        await network.enqueue { _, _ in
            ok(pull(scope: scope, cursor: 1, events: [log(try message("bad"), cursor: 1)]))
        }
        let first = await client.readStep()
        XCTAssertEqual(first, .progress)
        await store.failNextInstall(.inapplicable)
        await network.enqueue { _, request in
            ok(snapshot(scope: scope, attempt: try request.string("attempt"), cursor: 1))
        }
        let second = await client.readStep()
        let state = try await client.state()
        let archives = await store.archiveCount()
        XCTAssertEqual(second, .paused("inapplicable"))
        XCTAssertEqual(state.cursor, 0)
        XCTAssertEqual(archives, 1)
    }

    func testNoopAndEmptyMessageAreDifferentAndMismatchedOwnHashArchivesWork() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let client = try await SyncClient(store: store, transport: network)
        let operation = try message("own")
        _ = try await client.prepare(operation, expectedRevision: client.state().revision)
        await network.enqueue { _, _ in
            ok(
                pull(
                    scope: scope, cursor: 3,
                    events: [
                        log(nil, cursor: 1, reason: "already_applied"),
                        log(try message(""), cursor: 2),
                        log(
                            try message("canonical"), cursor: 3,
                            pending: Pending(
                                op: operation.id, hash: String(repeating: "a", count: 64))),
                    ]))
        }
        let work = await client.readStep()
        let state = try await client.state()
        let values = await store.values()
        let archives = await store.archiveCount()
        XCTAssertEqual(work, .idle)
        XCTAssertEqual(state.cursor, 3)
        XCTAssertEqual(values, ["", "canonical"])
        XCTAssertNil(state.prepared)
        XCTAssertNil(state.readPause)
        XCTAssertEqual(state.sendPause, "operation_changed")
        XCTAssertEqual(archives, 1)
    }
}

func ok(_ value: JSON) -> HTTPReply { HTTPReply(status: 200, body: value.encoded()) }
func fail(_ error: String, _ status: Int) -> HTTPReply {
    HTTPReply(status: status, body: JSON.object(["error": .string(error)]).encoded())
}
func baseState() -> ClientState {
    var state = ClientState(scope: try! testScope())
    state.cursor = 0
    return state
}

actor ScriptedTransport: SyncTransport {
    typealias Handler = @Sendable (String, [String: JSON]) async throws -> HTTPReply
    private var handlers: [Handler] = []
    private var requests = 0
    func enqueue(_ handler: @escaping Handler) { handlers.append(handler) }
    func count() -> Int { requests }
    func post(method: String, body: Data) async throws -> HTTPReply {
        requests += 1
        guard !handlers.isEmpty else { throw SyncError.invalid("unexpected_request") }
        return try await handlers.removeFirst()(method, Wire.object(JSON.parse(body)))
    }
}

private final class TestClock: @unchecked Sendable {
    private let lock = NSLock()
    private var time: TimeInterval = 1
    func now() -> TimeInterval { lock.withLock { time } }
    func date() -> Date { Date(timeIntervalSince1970: now()) }
    func advance(_ delta: TimeInterval) { lock.withLock { time += delta } }
}

private actor ResponseGate {
    private var continuation: CheckedContinuation<Void, Never>?

    func hold() async {
        await withCheckedContinuation { continuation = $0 }
    }

    func waitUntilHeld() async {
        while continuation == nil { await Task.yield() }
    }

    func release() {
        continuation?.resume()
        continuation = nil
    }
}

actor TestStore: SyncStore {
    struct Disk: Codable {
        var state: ClientState
        var base: [String] = []
        var intents: [String] = []
        var archives = 0
    }
    private var disk: Disk
    private let file: URL?
    private var failPage: SyncError?
    private var failInstall: SyncError?
    init(_ state: ClientState, file: URL? = nil) {
        self.disk = Disk(state: state)
        self.file = file
    }
    private init(_ disk: Disk, file: URL) {
        self.disk = disk
        self.file = file
    }
    static func open(_ file: URL) throws -> TestStore {
        TestStore(try JSONDecoder().decode(Disk.self, from: Data(contentsOf: file)), file: file)
    }
    func load() -> ClientState { disk.state }
    func values() -> [String] { disk.base }
    func intents() -> [String] { disk.intents }
    func archiveCount() -> Int { disk.archives }
    func edit(_ value: String) {
        disk.intents.append(value)
        disk.state = disk.state.edited()
    }
    func replace(_ state: ClientState) {
        disk.state = state
        disk.base = []
        disk.intents = []
    }
    func failNextPage(_ failure: SyncError) { failPage = failure }
    func failNextInstall(_ failure: SyncError) { failInstall = failure }
    func commit(_ transaction: Transition) throws -> Bool {
        guard disk.state == transaction.expected else { return false }
        var candidate = disk
        let settlements: [Settlement]
        switch transaction.change {
        case .metadata, .prepare: settlements = []
        case .receipt(_, let values): settlements = values
        case .page(let page, let values):
            for event in page.events {
                if let message = event.message {
                    candidate.base.append(String(decoding: message.data, as: UTF8.self))
                }
            }
            if let failure = failPage {
                failPage = nil
                throw failure
            }
            settlements = values
        case .snapshot(let items, _, let values):
            candidate.base = items.map { String(decoding: $0.data, as: UTF8.self) }
            if let failure = failInstall {
                failInstall = nil
                throw failure
            }
            settlements = values
        case .archive:
            candidate.archives += 1
            settlements = []
            if transaction.next.scope != transaction.expected.scope {
                candidate.base = []
                candidate.intents = []
            }
        }
        for settlement in settlements {
            if case .disputed = settlement { candidate.archives += 1 }
            if !candidate.intents.isEmpty { candidate.intents.removeFirst() }
        }
        candidate.state = transaction.next
        if let file { try JSONEncoder().encode(candidate).write(to: file, options: .atomic) }
        disk = candidate
        return true
    }
}
