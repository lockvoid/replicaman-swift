/// The application's storage boundary, shared by clients for different streams.
public struct ClientConfiguration: Sendable {
    /// Return the persisted state for this stream in the current local store.
    /// A fresh stream returns `ClientState(scope:)` with no confirmed base.
    public let load: @Sendable (String) async throws -> ClientState

    /// Compare the full expected state, apply `change`, and save `next` in one
    /// transaction. Return false for a version/context race. Throwing must roll
    /// back everything. If `validUntilUptime` expires before saving, roll back
    /// and return false. Never split application and metadata into separate saves.
    public let commit: @Sendable (Transition) async throws -> Bool

    public init(
        load: @escaping @Sendable (String) async throws -> ClientState,
        commit: @escaping @Sendable (Transition) async throws -> Bool
    ) {
        self.load = load
        self.commit = commit
    }
}
