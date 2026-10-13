import Foundation

public struct PreparedOperation: Codable, Equatable, Sendable {
    public let message: CloudEvent
    public let hash: String
    public internal(set) var outcome: Outcome?
    public internal(set) var cancel: Bool
    public var pending: Pending { Pending(op: message.id, hash: hash) }
}

public struct Recovery: Codable, Equatable, Sendable {
    public let attempt: String
    public let pending: [Pending]
    public let minimumCursor: Int64
    public let limit: Int
}

public struct RetryState: Codable, Equatable, Sendable {
    public internal(set) var failures = 0
    public internal(set) var notBefore: Date?
}

/// Persist alongside application data. A nil cursor means no confirmed base.
/// Updating an application intent must also change `revision` with `edited()`.
public struct ClientState: Codable, Equatable, Sendable {
    public let scope: Scope
    /// The current SDK instance's local ownership token; never sent to the server.
    public internal(set) var lease: String?
    public internal(set) var session: String
    public internal(set) var revision: String
    public internal(set) var cursor: Int64?
    public internal(set) var head: Int64
    public internal(set) var prepared: PreparedOperation?
    public internal(set) var lastReceipt: Receipt?
    public internal(set) var recovery: Recovery?
    public internal(set) var readPause: String?
    public internal(set) var sendPause: String?
    public internal(set) var repairedBase: Bool
    public internal(set) var readRetry = RetryState()
    public internal(set) var sendRetry = RetryState()

    public init(scope: Scope) {
        self.scope = scope
        session = Wire.uuid()
        revision = Wire.uuid()
        cursor = nil
        head = 0
        prepared = nil
        lastReceipt = nil
        recovery = nil
        readPause = nil
        sendPause = nil
        repairedBase = false
    }

    public func edited() -> ClientState {
        var next = self
        next.revision = Wire.uuid()
        return next
    }

    func validated() throws -> ClientState {
        _ = try Scope(
            subject: scope.subject, stream: scope.stream, epoch: scope.epoch, client: scope.client)
        try Wire.uuid(session)
        try Wire.uuid(revision)
        if let lease { try Wire.uuid(lease) }
        for retry in [readRetry, sendRetry] {
            try Wire.check(retry.failures >= 0, "saved_retry")
            if let deadline = retry.notBefore {
                try Wire.check(deadline.timeIntervalSince1970.isFinite, "saved_retry")
            }
        }
        try Wire.check(head >= 0 && (cursor == nil || cursor! >= 0), "saved_position")
        if let prepared {
            try Wire.check(prepared.hash == prepared.message.hash, "saved_operation")
        }
        if let recovery {
            try Wire.uuid(recovery.attempt)
            try Wire.check(
                recovery.pending.count <= 1 && (1...100).contains(recovery.limit), "saved_pending")
            for item in recovery.pending {
                try Wire.uuid(item.op)
                try Wire.hash(item.hash)
            }
            if let prepared {
                try Wire.check(recovery.pending.contains(prepared.pending), "snapshot_barrier")
            }
        }
        return self
    }
}

public enum Settlement: Equatable, Sendable {
    /// Remove the corresponding optimistic edit and reconcile subsequent intents.
    case decided(Receipt)
    /// Archive the original operation and dependent work; keep confirmed reads working.
    case disputed(PreparedOperation)
}

public enum ApplicationChange: Sendable {
    case metadata
    /// Atomically associate this immutable operation with the chosen local intent.
    case prepare(CloudEvent)
    /// Acceptance alone leaves the optimistic edit in place until covered.
    case receipt(Receipt, settlements: [Settlement])
    case page(PullPage, settlements: [Settlement])
    /// Rebuild from all items, including current unprepared local intents in reconciliation.
    case snapshot(items: [CloudEvent], coverage: [OperationCoverage], settlements: [Settlement])
    /// Save original data/queue for recovery before replacing its scope or repairing it.
    case archive(reason: String)
}

public struct Transition: Sendable {
    public let expected: ClientState
    public let next: ClientState
    public let change: ApplicationChange
    /// Snapshot installation must still be valid immediately before commit.
    /// Compare with ProcessInfo.processInfo.systemUptime; never persist this value.
    public let validUntilUptime: TimeInterval?

    init(
        expected: ClientState, next: ClientState, change: ApplicationChange,
        validUntilUptime: TimeInterval? = nil
    ) {
        self.expected = expected
        self.next = next
        self.change = change
        self.validUntilUptime = validUntilUptime
    }
}

enum StateChange {
    static func receipt(_ receipt: Receipt, in state: ClientState) throws -> (
        ClientState, [Settlement]
    ) {
        var next = state
        var settled: [Settlement] = []
        if let old = state.lastReceipt, old.op == receipt.op {
            try Wire.check(old == receipt, "contradictory_receipt")
        }
        guard var pending = state.prepared, pending.message.id == receipt.op else {
            return (next, settled)
        }
        try Wire.check(pending.hash == receipt.hash, "receipt_hash")
        if let old = pending.outcome {
            try Wire.check(old == receipt.outcome, "contradictory_receipt")
        }
        pending.outcome = receipt.outcome
        next.prepared = pending
        switch receipt.outcome {
        case .rejected:
            next.prepared = nil
            settled.append(.decided(receipt))
        case .accepted(let cursor, _):
            if let confirmed = state.cursor, cursor <= confirmed {
                next.prepared = nil
                settled.append(.decided(receipt))
            }
        }
        next.lastReceipt = receipt
        return (next, settled)
    }

    static func page(_ page: PullPage, in state: ClientState) throws -> (ClientState, [Settlement])
    {
        var next = state
        var settlements: [Settlement] = []
        // Reconciliation is committed with ALL canonical events, including our own.
        next.cursor = page.cursor
        next.head = max(state.head, page.head)
        for event in page.events {
            guard let op = event.op, let hash = event.hash else { continue }
            let result = Receipt(op: op, hash: hash, outcome: event.outcome)
            if let old = next.lastReceipt, old.op == op, old.hash == hash {
                try Wire.check(old == result, "contradictory_receipt")
            }
            guard let pending = next.prepared, pending.message.id == op else { continue }
            if pending.hash != hash {
                settlements.append(.disputed(pending))
                next.prepared = nil
                next.sendPause = "operation_changed"
            } else {
                let updated = try receipt(result, in: next)
                next = updated.0
                settlements += updated.1
            }
        }
        // A canonical result may have been read before a delayed receipt arrived.
        if let pending = next.prepared, let outcome = pending.outcome {
            let updated = try receipt(
                Receipt(op: pending.message.id, hash: pending.hash, outcome: outcome), in: next)
            next = updated.0
            settlements += updated.1
        }
        return (next, settlements)
    }

    static func snapshot(_ page: SnapshotPage, in state: ClientState) throws -> (
        ClientState, [Settlement]
    ) {
        var next = state
        var settlements: [Settlement] = []
        next.cursor = page.cursor
        next.head = max(state.head, page.cursor)
        for check in page.ops {
            let old = next.prepared
            if let last = next.lastReceipt, last.op == check.pending.op {
                switch check.coverage {
                case .decided(let outcome):
                    try Wire.check(
                        last.hash == check.pending.hash && last.outcome == outcome,
                        "contradictory_receipt")
                case .mismatch: throw SyncError.invalid("contradictory_receipt")
                case .unseen:
                    if let accepted = last.outcome.cursor {
                        try Wire.check(accepted > page.cursor, "unseen_in_snapshot")
                    }
                }
            }
            guard let old, old.pending == check.pending else { continue }
            switch check.coverage {
            case .decided(let outcome):
                let updated = try receipt(
                    Receipt(op: check.pending.op, hash: check.pending.hash, outcome: outcome),
                    in: next)
                next = updated.0
                settlements += updated.1
            case .unseen:
                // A receipt obtained after snapshot publication wins over unseen.
                if let outcome = old.outcome {
                    if let position = outcome.cursor {
                        try Wire.check(position > page.cursor, "unseen_in_snapshot")
                    }
                    let updated = try receipt(
                        Receipt(op: old.message.id, hash: old.hash, outcome: outcome), in: next)
                    next = updated.0
                    settlements += updated.1
                }
            case .mismatch:
                settlements.append(.disputed(old))
                next.prepared = nil
                next.sendPause = "operation_changed"
            }
        }
        next.recovery = nil
        return (next, settlements)
    }
}
