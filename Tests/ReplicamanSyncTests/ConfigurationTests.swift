import Foundation
import XCTest

@testable import ReplicamanSync

@MainActor
final class ConfigurationTests: XCTestCase {
    func testSharedConfigurationRoutesStreamsAndReplacesOnlyOneLease() async throws {
        let firstScope = try testScope()
        let secondScope = try Scope(
            subject: firstScope.subject, stream: "catalog/system", epoch: firstScope.epoch,
            client: firstScope.client)
        var secondState = ClientState(scope: secondScope)
        secondState.cursor = 0
        let firstStore = TestStore(baseState())
        let secondStore = TestStore(secondState)
        let stores = [firstScope.stream: firstStore, secondScope.stream: secondStore]
        let configuration = ClientConfiguration(
            load: { stream in
                guard let store = stores[stream] else { throw SyncError.storage("unknown_stream") }
                return await store.load()
            },
            commit: { transition in
                XCTAssertEqual(transition.expected.scope.stream, transition.next.scope.stream)
                guard let store = stores[transition.expected.scope.stream] else {
                    throw SyncError.storage("unknown_stream")
                }
                return try await store.commit(transition)
            })
        let network = ScriptedTransport()
        let first = try await SyncClient(
            stream: firstScope.stream, configuration: configuration, transport: network)
        let second = try await SyncClient(
            stream: secondScope.stream, configuration: configuration, transport: network)
        await network.enqueue { _, request in
            XCTAssertEqual(request["stream"], .string(firstScope.stream))
            return ok(
                pull(
                    scope: firstScope, cursor: 1,
                    events: [log(try message("private"), cursor: 1)]))
        }
        await network.enqueue { _, request in
            XCTAssertEqual(request["stream"], .string(secondScope.stream))
            return ok(
                pull(
                    scope: secondScope, cursor: 2,
                    events: [log(try message("system"), cursor: 2)]))
        }
        let firstRead = await first.readStep()
        let secondRead = await second.readStep()
        XCTAssertEqual(firstRead, .idle)
        XCTAssertEqual(secondRead, .idle)

        let replacement = try await SyncClient(
            stream: firstScope.stream, configuration: configuration, transport: network)
        let obsolete = await first.readStep()
        let replacedState = try await replacement.state()
        let unaffectedState = try await second.state()
        let firstValues = await firstStore.values()
        let secondValues = await secondStore.values()
        let requests = await network.count()
        XCTAssertEqual(obsolete, .stopped)
        XCTAssertEqual(replacedState.cursor, 1)
        XCTAssertEqual(unaffectedState.cursor, 2)
        XCTAssertEqual(firstValues, ["private"])
        XCTAssertEqual(secondValues, ["system"])
        XCTAssertEqual(requests, 2)
    }

    func testWrongStreamLoadCannotClaimALease() async throws {
        let state = baseState()
        let configuration = ClientConfiguration(
            load: { stream in
                XCTAssertEqual(stream, "catalog/wrong")
                return state
            },
            commit: { _ in
                XCTFail("A mismatched stream must never reach commit")
                return true
            })
        let network = ScriptedTransport()
        do {
            _ = try await SyncClient(
                stream: "catalog/wrong", configuration: configuration, transport: network)
            XCTFail("Accepted another stream's state")
        } catch {
            XCTAssertEqual(error as? SyncError, .invalid("stream_mismatch"))
        }
        let requests = await network.count()
        XCTAssertEqual(requests, 0)
    }

    func testWrongStreamOnLaterLoadIsRejectedEvenWithTheCurrentLease() async throws {
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let client = try await SyncClient(
            stream: try testScope().stream, configuration: store.configuration, transport: network)
        let original = try await client.state()
        let foreignScope = try Scope(
            subject: original.scope.subject, stream: "catalog/foreign", epoch: original.scope.epoch,
            client: original.scope.client)
        var foreign = ClientState(scope: foreignScope)
        foreign.lease = original.lease
        foreign.session = original.session
        await store.replace(foreign)

        do {
            _ = try await client.state()
            XCTFail("Accepted another stream after construction")
        } catch {
            XCTAssertEqual(error as? SyncError, .invalid("stream_mismatch"))
        }
        let work = await client.readStep()
        let requests = await network.count()
        let persisted = await store.load()
        XCTAssertEqual(work, .paused("storage_error"))
        XCTAssertEqual(requests, 0)
        XCTAssertEqual(persisted, foreign)
    }

    func testReplaceScopeRejectsAStreamChangeWithoutArchiving() async throws {
        let store = TestStore(baseState())
        let client = try await SyncClient(
            stream: try testScope().stream, configuration: store.configuration,
            transport: ScriptedTransport())
        await store.edit("keep this intent")
        let original = try await client.state()
        let other = try Scope(
            subject: original.scope.subject, stream: "catalog/other", epoch: original.scope.epoch,
            client: original.scope.client)
        do {
            try await client.replaceScope(other, reason: "other stream")
            XCTFail("Changed an immutable stream key")
        } catch {
            XCTAssertEqual(error as? SyncError, .invalid("stream_mismatch"))
        }
        let persisted = try await client.state()
        let intents = await store.intents()
        let archives = await store.archiveCount()
        XCTAssertEqual(persisted, original)
        XCTAssertEqual(intents, ["keep this intent"])
        XCTAssertEqual(archives, 0)
    }

    func testSameStreamScopeReplacementArchivesAndDiscardsOldReply() async throws {
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let client = try await SyncClient(
            stream: try testScope().stream, configuration: store.configuration, transport: network)
        await store.edit("old account intent")
        let previous = try await client.state()
        let gate = ResponseGate()
        await network.enqueue { _, _ in
            await gate.hold()
            return HTTPReply(status: 200, body: Data("malformed old response".utf8))
        }
        let inFlight = Task { await client.readStep() }
        await gate.waitUntilHeld()
        let scope = try Scope(
            subject: "user:2", stream: previous.scope.stream, epoch: Wire.uuid(),
            client: Wire.uuid())
        try await client.replaceScope(scope, reason: "account and epoch changed")
        await gate.release()
        let oldResult = await inFlight.value
        let replaced = try await client.state()
        let archives = await store.archiveCount()
        let intents = await store.intents()
        XCTAssertEqual(oldResult, .stale)
        XCTAssertEqual(replaced.scope, scope)
        XCTAssertEqual(replaced.lease, previous.lease)
        XCTAssertNotEqual(replaced.session, previous.session)
        XCTAssertNil(replaced.cursor)
        XCTAssertNil(replaced.readPause)
        XCTAssertEqual(archives, 1)
        XCTAssertEqual(intents, [])

        await network.enqueue { method, request in
            XCTAssertEqual(method, "snapshot")
            XCTAssertEqual(request["stream"], .string(scope.stream))
            XCTAssertEqual(request["epoch"], .string(scope.epoch))
            return ok(
                snapshot(
                    scope: scope, attempt: try request.string("attempt"), cursor: 0,
                    items: [try message("new account base")]))
        }
        let freshRead = await client.readStep()
        let values = await store.values()
        XCTAssertEqual(freshRead, .progress)
        XCTAssertEqual(values, ["new account base"])
    }

    func testCommitRetriesACompareAndSwapRaceWithoutLosingTheLocalEdit() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let gate = ResponseGate()
        let configuration = ClientConfiguration(
            load: { _ in await store.load() },
            commit: { transition in
                if case .page = transition.change { await gate.holdOnce() }
                return try await store.commit(transition)
            })
        let network = ScriptedTransport()
        await network.enqueue { _, _ in
            ok(
                pull(
                    scope: scope, cursor: 1,
                    events: [log(try message("canonical"), cursor: 1)]))
        }
        let client = try await SyncClient(
            stream: scope.stream, configuration: configuration, transport: network)
        let reading = Task { await client.readStep() }
        await gate.waitUntilHeld()
        await store.edit("created during commit")
        await gate.release()

        let result = await reading.value
        let state = try await client.state()
        let values = await store.values()
        let intents = await store.intents()
        let requests = await network.count()
        XCTAssertEqual(result, .idle)
        XCTAssertEqual(state.cursor, 1)
        XCTAssertEqual(values, ["canonical"])
        XCTAssertEqual(intents, ["created during commit"])
        XCTAssertEqual(requests, 1)
    }

    func testSnapshotDeadlineIsPassedToCommitAndExpiredInstallStartsAnotherAttempt() async throws {
        let scope = try testScope()
        let store = TestStore(baseState())
        let network = ScriptedTransport()
        let clock = TestClock()
        let gate = ResponseGate()
        let configuration = ClientConfiguration(
            load: { _ in await store.load() },
            commit: { transition in
                if case .snapshot = transition.change {
                    let deadline = try XCTUnwrap(transition.validUntilUptime)
                    await gate.holdOnce()
                    guard clock.now() < deadline else { return false }
                } else {
                    XCTAssertNil(transition.validUntilUptime)
                }
                return try await store.commit(transition)
            })
        let client = try await SyncClient(
            stream: scope.stream, configuration: configuration, transport: network, now: clock.now)
        await network.enqueue { _, _ in
            ok(
                pull(
                    scope: scope, cursor: 1,
                    events: [log(try message("retained baseline"), cursor: 1)]))
        }
        _ = await client.readStep()
        await network.enqueue { _, _ in fail("cursor_expired", 409) }
        _ = await client.readStep()
        let previous = try await client.state()
        let oldAttempt = try XCTUnwrap(previous.recovery?.attempt)
        await network.enqueue { _, request in
            XCTAssertEqual(request["attempt"], .string(oldAttempt))
            return ok(
                snapshot(
                    scope: scope, attempt: oldAttempt, cursor: 2,
                    items: [try message("expired image")], ttl: 1))
        }
        let reading = Task { await client.readStep() }
        await gate.waitUntilHeld()
        clock.advance(1)
        await gate.release()

        let expired = await reading.value
        let restarted = try await client.state()
        let retained = await store.values()
        XCTAssertEqual(expired, .progress)
        XCTAssertEqual(restarted.cursor, 1)
        XCTAssertNotEqual(restarted.recovery?.attempt, oldAttempt)
        XCTAssertEqual(retained, ["retained baseline"])

        await network.enqueue { _, request in
            XCTAssertNotEqual(request["attempt"], .string(oldAttempt))
            return ok(
                snapshot(
                    scope: scope, attempt: try request.string("attempt"), cursor: 3,
                    items: [try message("fresh image")]))
        }
        let fresh = await client.readStep()
        let state = try await client.state()
        let values = await store.values()
        XCTAssertEqual(fresh, .progress)
        XCTAssertEqual(state.cursor, 3)
        XCTAssertNil(state.recovery)
        XCTAssertEqual(values, ["fresh image"])
    }
}
