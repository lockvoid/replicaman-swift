import Foundation

public enum Work: Equatable, Sendable {
    case progress, idle, stale, stopped
    case retry(after: TimeInterval, failures: Int)
    case paused(String)
}
public enum Lane: Sendable { case read, send }

public struct ClientLimits: Sendable {
    public var snapshotItems = 2048
    public var snapshotBytes = 64 * 1024 * 1024
    public var pageItems = 100
    public init() {}
}

/// One actor per stream. `runReads` and `runSender` may run concurrently;
/// a failed outbound request never prevents confirmed reads.
public actor SyncClient {
    private let store: any SyncStore
    private let transport: any SyncTransport
    private let limits: ClientLimits
    private let now: @Sendable () -> TimeInterval
    private let wallNow: @Sendable () -> Date
    private let jitter: @Sendable () -> Double
    private let lease = Wire.uuid()
    private var reading = false
    private var sending = false
    private var readFailures = 0
    private var sendFailures = 0
    private var readAfter: TimeInterval = 0
    private var sendAfter: TimeInterval = 0
    private var target: (session: String, cursor: Int64, to: Int64)?
    private var assembly: Assembly?

    private struct Assembly {
        let session: String
        let attempt: String
        let first: SnapshotPage
        var items: [CloudEvent]
        var bytes: Int
        var next: String?
        var tokens: Set<String>
        var ids: Set<String>
        var deadline: TimeInterval
    }

    public init(
        store: any SyncStore, transport: any SyncTransport, limits: ClientLimits = .init(),
        now: @escaping @Sendable () -> TimeInterval = { ProcessInfo.processInfo.systemUptime },
        wallNow: @escaping @Sendable () -> Date = { Date() },
        jitter: @escaping @Sendable () -> Double = { Double.random(in: 0.5...1.0) }
    ) async throws {
        try Wire.check(
            (1...100).contains(limits.pageItems) && limits.snapshotItems > 0
                && limits.snapshotBytes > 0, "limits")
        self.store = store
        self.transport = transport
        self.limits = limits
        self.now = now
        self.wallNow = wallNow
        self.jitter = jitter

        // Reopening a store replaces its previous client. The persisted token
        // makes both old responses and future work from that instance harmless.
        while true {
            let current = try await store.load().validated()
            var next = current.edited()
            next.lease = lease
            if try await store.commit(Transition(expected: current, next: next, change: .metadata))
            {
                readFailures = next.readRetry.failures
                sendFailures = next.sendRetry.failures
                readAfter =
                    now() + max(0, next.readRetry.notBefore?.timeIntervalSince(wallNow()) ?? 0)
                sendAfter =
                    now() + max(0, next.sendRetry.notBefore?.timeIntervalSince(wallNow()) ?? 0)
                return
            }
        }
    }

    public func state() async throws -> ClientState {
        let current = try await store.load().validated()
        guard current.lease == lease else { throw SyncError.superseded }
        return current
    }

    /// The app encodes a reconciled intent against this exact local revision.
    /// `commit(.prepare)` associates it with the intent in the same transaction.
    @discardableResult
    public func prepare(_ message: CloudEvent, expectedRevision: String) async throws -> Bool {
        let current = try await state()
        guard current.revision == expectedRevision else { return false }
        guard current.recovery == nil, current.prepared == nil, current.cursor != nil,
            current.sendPause == nil
        else { throw SyncError.busy }
        try Wire.check(current.lastReceipt?.op != message.id, "reused_operation")
        var next = current.edited()
        next.prepared = PreparedOperation(
            message: message, hash: message.hash, outcome: nil, cancel: false)
        return try await store.commit(
            Transition(expected: current, next: next, change: .prepare(message)))
    }

    public func cancel() async throws {
        while true {
            let current = try await state()
            guard var pending = current.prepared else { return }
            pending.cancel = true
            var next = current.edited()
            next.prepared = pending
            next.sendPause = nil
            if try await store.commit(Transition(expected: current, next: next, change: .metadata))
            {
                return
            }
        }
    }

    /// Call after repairing storage, credentials, or codec support. This never
    /// replaces a prepared operation's identity or infers a rejection.
    public func resume(_ lane: Lane) async throws {
        while true {
            let current = try await state()
            var next = current.edited()
            switch lane {
            case .read:
                next.readPause = nil
                next.repairedBase = false
            case .send: next.sendPause = nil
            }
            if try await store.commit(Transition(expected: current, next: next, change: .metadata))
            {
                return
            }
        }
    }

    /// The host must archive/isolate original editable data and queue inside
    /// this transition. Catalog hosts may discard their reproducible cache.
    public func replaceScope(_ scope: Scope, reason: String) async throws {
        while true {
            let current = try await state()
            var next = ClientState(scope: scope)
            next.lease = lease
            if try await store.commit(
                Transition(expected: current, next: next, change: .archive(reason: reason)))
            {
                assembly = nil
                target = nil
                readAfter = 0
                sendAfter = 0
                readFailures = 0
                sendFailures = 0
                return
            }
        }
    }

    public func readStep() async -> Work {
        guard !reading else { return .idle }
        reading = true
        defer { reading = false }
        var session: String?
        do {
            var current = try await state()
            session = current.session
            if let reason = current.readPause { return .paused(reason) }
            if now() < readAfter {
                return .retry(after: readAfter - now(), failures: readFailures)
            }
            if current.cursor == nil && current.recovery == nil {
                guard try await beginSnapshot(current, damaged: false) else { return .stale }
                current = try await state()
                session = current.session
            }
            let result: Work
            if current.recovery != nil {
                result = try await snapshotStep(current)
            } else {
                result = try await pullStep(current)
            }
            if result != .stale {
                try await clearRetry(.read, session: current.session)
            }
            return result
        } catch is CancellationError { return .idle } catch {
            return await failed(error, lane: .read, session: session)
        }
    }

    public func sendStep() async -> Work {
        guard !sending else { return .idle }
        sending = true
        defer { sending = false }
        var session: String?
        do {
            let initial = try await state()
            session = initial.session
            if let reason = initial.sendPause { return .paused(reason) }
            if now() < sendAfter {
                return .retry(after: sendAfter - now(), failures: sendFailures)
            }
            guard let pending = initial.prepared, pending.outcome == nil else { return .idle }
            var request = initial.scope.fields
            if pending.cancel {
                request["cancel"] = pending.pending.json
            } else {
                request["message"] = pending.message.json
            }
            let reply = try await exchange("push", .object(request))
            guard try await state().session == initial.session else { return .stale }
            let fields = try Wire.object(JSON.parse(reply.success()))
            try Wire.context(fields, initial.scope)
            try Wire.check(try fields.string("op") == pending.message.id, "receipt_operation")
            let receipt = Receipt(
                op: pending.message.id, hash: pending.hash, outcome: try Outcome.parse(fields))
            while true {
                let current = try await state()
                guard current.session == initial.session else { return .stale }
                var (updated, settlements) = try StateChange.receipt(receipt, in: current)
                updated.sendRetry = RetryState()
                if try await store.commit(
                    Transition(
                        expected: current, next: updated.edited(),
                        change: .receipt(receipt, settlements: settlements)))
                {
                    sendFailures = 0
                    sendAfter = 0
                    return .progress
                }
            }
        } catch is CancellationError { return .idle } catch {
            return await failed(error, lane: .send, session: session)
        }
    }

    /// Loops retry forever after transient failures. The observer receives the
    /// eighth and subsequent failures; that is a status, never a durable pause.
    public func runReads(
        pollInterval: TimeInterval = 30,
        observe: @escaping @Sendable (Work) async -> Void = { _ in }
    ) async {
        await run(lane: .read, idle: pollInterval, observe: observe)
    }
    public func runSender(
        pollInterval: TimeInterval = 1, observe: @escaping @Sendable (Work) async -> Void = { _ in }
    ) async {
        await run(lane: .send, idle: pollInterval, observe: observe)
    }
    private func run(
        lane: Lane, idle: TimeInterval, observe: @escaping @Sendable (Work) async -> Void
    ) async {
        while !Task.isCancelled {
            let work = lane == .read ? await readStep() : await sendStep()
            await observe(work)
            if work == .stopped { return }
            let delay: TimeInterval
            switch work {
            case .progress, .stale: delay = 0
            case .retry(let after, _): delay = after
            case .idle, .paused: delay = max(0.05, idle)
            case .stopped: return
            }
            // Keep an absolute retry gate, but sleep in bounded pieces. This
            // also avoids Duration overflow for a very large Retry-After.
            do { try await Task.sleep(for: .seconds(min(60, delay))) } catch { return }
        }
    }

    private func exchange(_ method: String, _ request: JSON) async throws -> HTTPReply {
        do { return try await transport.post(method: method, body: request.encoded()) } catch let
            error as SyncError
        { throw error } catch is CancellationError { throw CancellationError() } catch {
            throw SyncError.transient(retryAfter: nil)
        }
    }

    private func pullStep(_ initial: ClientState) async throws -> Work {
        guard let cursor = initial.cursor else { throw SyncError.invalid("missing_base") }
        var fields = initial.scope.fields
        fields["cursor"] = .string(String(cursor))
        fields["limit"] = .number(String(limits.pageItems))
        let to = target.flatMap {
            $0.session == initial.session && $0.cursor == cursor ? $0.to : nil
        }
        if let to { fields["to"] = .string(String(to)) }
        let reply = try await exchange("pull", .object(fields))
        let live = try await state()
        guard live.session == initial.session && live.cursor == cursor && live.recovery == nil
        else { return .stale }
        let page = try PullPage(
            data: reply.success(), scope: initial.scope, after: cursor, to: to,
            limit: limits.pageItems)
        try Wire.check(page.head >= initial.head, "head_regressed")
        while true {
            let current = try await state()
            guard
                current.session == initial.session && current.cursor == cursor
                    && current.recovery == nil
            else { return .stale }
            let (updated, settlements) = try StateChange.page(page, in: current)
            if try await store.commit(
                Transition(
                    expected: current, next: updated.edited(),
                    change: .page(page, settlements: settlements)))
            {
                target = page.more ? (initial.session, page.cursor, page.to) : nil
                return page.more ? .progress : .idle
            }
        }
    }

    private func beginSnapshot(_ current: ClientState, damaged: Bool) async throws -> Bool {
        var next = current.edited()
        next.recovery = Recovery(
            attempt: Wire.uuid(), pending: current.prepared.map { [$0.pending] } ?? [],
            minimumCursor: damaged ? 0 : current.cursor ?? 0, limit: limits.pageItems)
        if damaged {
            next.session = Wire.uuid()
            next.repairedBase = true
            next.head = 0
        }
        let change: ApplicationChange = damaged ? .archive(reason: "damaged_base") : .metadata
        let committed = try await store.commit(
            Transition(expected: current, next: next, change: change))
        if committed {
            assembly = nil
            target = nil
        }
        return committed
    }

    private func snapshotStep(_ initial: ClientState) async throws -> Work {
        guard let recovery = initial.recovery else { throw SyncError.invalid("missing_recovery") }
        if assembly?.session != initial.session || assembly?.attempt != recovery.attempt {
            assembly = nil
        }
        if let saved = assembly, now() >= saved.deadline {
            return try await restartSnapshot(initial)
        }
        if let saved = assembly, saved.next == nil {
            return try await install(saved, initial: initial, recovery: recovery)
        }
        var fields = initial.scope.fields
        fields["client"] = .string(initial.scope.client)
        fields["attempt"] = .string(recovery.attempt)
        fields["limit"] = .number(String(recovery.limit))
        if let saved = assembly, let token = saved.next {
            fields["snapshot"] = .string(saved.first.snapshot)
            fields["page"] = .string(token)
        } else {
            fields["page"] = .null
            fields["pending"] = .array(recovery.pending.map(\.json))
        }
        let start = now()
        let reply = try await exchange("snapshot", .object(fields))
        let live = try await state()
        guard live.session == initial.session && live.recovery == recovery else { return .stale }
        let page = try SnapshotPage(
            data: reply.success(), scope: initial.scope, attempt: recovery.attempt,
            pending: recovery.pending, limit: recovery.limit)
        let deadline = start + Double(page.ttl)
        if now() >= deadline { return try await restartSnapshot(live) }
        try Wire.check(page.cursor >= recovery.minimumCursor, "snapshot_regressed")
        var saved =
            assembly
            ?? Assembly(
                session: initial.session, attempt: recovery.attempt, first: page,
                items: [], bytes: 0, next: nil, tokens: [], ids: [], deadline: deadline)
        try Wire.check(
            saved.first.snapshot == page.snapshot && saved.first.cursor == page.cursor
                && saved.first.ops == page.ops, "snapshot_changed")
        if let next = page.next {
            try Wire.check(saved.tokens.insert(next).inserted, "snapshot_loop")
        }
        for item in page.items {
            try Wire.check(saved.ids.insert(item.id).inserted, "snapshot_item_id")
            try Wire.check(item.data.count <= limits.snapshotBytes - saved.bytes, "snapshot_bytes")
            saved.bytes += item.data.count
            saved.items.append(item)
            try Wire.check(saved.items.count <= limits.snapshotItems, "snapshot_items")
        }
        saved.next = page.next
        saved.deadline = deadline
        assembly = saved
        if page.next != nil { return .progress }
        return try await install(saved, initial: initial, recovery: recovery)
    }

    private func install(_ saved: Assembly, initial: ClientState, recovery: Recovery) async throws
        -> Work
    {
        while true {
            let current = try await state()
            guard current.session == initial.session && current.recovery == recovery else {
                assembly = nil
                return .stale
            }
            if now() >= saved.deadline { return try await restartSnapshot(current) }
            let (updated, settlements) = try StateChange.snapshot(saved.first, in: current)
            if try await store.commit(
                Transition(
                    expected: current, next: updated.edited(),
                    change: .snapshot(
                        items: saved.items, coverage: saved.first.ops, settlements: settlements),
                    validUntilUptime: saved.deadline))
            {
                assembly = nil
                return .progress
            }
        }
    }

    private func restartSnapshot(_ current: ClientState) async throws -> Work {
        var next = current.edited()
        next.session = Wire.uuid()
        next.recovery = Recovery(
            attempt: Wire.uuid(), pending: current.prepared.map { [$0.pending] } ?? [],
            minimumCursor: current.recovery?.minimumCursor ?? current.cursor ?? 0,
            limit: limits.pageItems)
        if try await store.commit(Transition(expected: current, next: next, change: .metadata)) {
            assembly = nil
            target = nil
            return .progress
        }
        return .stale
    }

    private func clearRetry(_ lane: Lane, session: String) async throws {
        while true {
            let current = try await state()
            guard current.session == session else { return }
            let retry = lane == .read ? current.readRetry : current.sendRetry
            if retry == RetryState() { return }

            var next = current.edited()
            if lane == .read {
                next.readRetry = RetryState()
            } else {
                next.sendRetry = RetryState()
            }
            if try await store.commit(Transition(expected: current, next: next, change: .metadata))
            {
                if lane == .read {
                    readFailures = 0
                    readAfter = 0
                } else {
                    sendFailures = 0
                    sendAfter = 0
                }
                return
            }
        }
    }

    private func scheduleRetry(_ lane: Lane, session: String?, minimumDelay: TimeInterval?)
        async throws -> Work
    {
        while true {
            let current = try await state()
            guard session == nil || current.session == session else { return .stale }
            let previous = lane == .read ? current.readRetry : current.sendRetry
            let failures = min(previous.failures, Int.max - 1) + 1
            let backoff = min(60, pow(2, Double(min(failures - 1, 6))))
            let delay = max(minimumDelay ?? 0, backoff * max(0.1, min(1, jitter())))
            let retry = RetryState(
                failures: failures, notBefore: wallNow().addingTimeInterval(delay))
            var next = current.edited()
            if lane == .read {
                next.readRetry = retry
            } else {
                next.sendRetry = retry
            }
            if try await store.commit(Transition(expected: current, next: next, change: .metadata))
            {
                if lane == .read {
                    readFailures = failures
                    readAfter = now() + delay
                } else {
                    sendFailures = failures
                    sendAfter = now() + delay
                }
                return .retry(after: delay, failures: failures)
            }
        }
    }

    private func failed(_ error: any Error, lane: Lane, session: String?) async -> Work {
        if error as? SyncError == .superseded {
            assembly = nil
            target = nil
            return .stopped
        }
        do {
            let current = try await state()
            if let session, session != current.session { return .stale }
            if case SyncError.transient(let retryAfter) = error {
                return try await scheduleRetry(lane, session: session, minimumDelay: retryAfter)
            }
            let code: String
            switch error {
            case SyncError.server(let reason, _, _): code = reason
            case SyncError.invalid(let reason): code = "protocol_" + reason
            case SyncError.inapplicable: code = "inapplicable"
            case SyncError.unsupportedFormat: code = "unsupported_format"
            default: code = "storage_error"
            }
            if lane == .read && code == "snapshot_expired" {
                return try await restartSnapshot(current)
            }
            if lane == .read && code == "cursor_expired" {
                return try await beginSnapshot(current, damaged: false) ? .progress : .stale
            }
            if lane == .read && ["cursor_invalid", "inapplicable"].contains(code)
                && !current.repairedBase
            {
                return try await beginSnapshot(current, damaged: true) ? .progress : .stale
            }
            var next = current.edited()
            if lane == .read { next.readPause = code } else { next.sendPause = code }
            if try await store.commit(Transition(expected: current, next: next, change: .metadata))
            {
                return .paused(code)
            }
            return .stale
        } catch SyncError.superseded {
            return .stopped
        } catch {
            return .paused("storage_error")
        }
    }
}
