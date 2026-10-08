import Foundation
import Network
import XCTest
import zlib

@testable import ReplicamanSync

@MainActor
final class TransportTests: XCTestCase {
    func testGzipResponseIsDecodedAndRequestUsesProtocolHeaders() async throws {
        let body = Data("{\"ok\":true}".utf8)
        let server = try await LoopbackHTTP.start(
            response: response(headers: ["Content-Encoding": "gzip"], body: gzip(body)))
        defer { server.stop() }
        let transport = try authorizedTransport(server)

        let reply = try await withinDeadline {
            try await transport.post(method: "pull", body: Data("{}".utf8))
        }

        XCTAssertEqual(reply.status, 200)
        XCTAssertEqual(reply.body, body)
        let request = try XCTUnwrap(server.requests.first)
        XCTAssertTrue(request.hasPrefix("POST /replicaman/v1/pull HTTP/1.1\r\n"))
        XCTAssertTrue(request.lowercased().contains("content-type: application/json\r\n"))
        XCTAssertTrue(request.lowercased().contains("cache-control: no-store\r\n"))
        XCTAssertTrue(request.contains("Bearer transport-test-secret"))
    }

    func testOversizedContentLengthIsRejectedBeforeReadingTheCompleteBody() async throws {
        // URLSession may deliver headers only after the first body byte. The
        // remaining declared bytes never arrive, so completion cannot hide a missing check.
        let server = try await LoopbackHTTP.start(
            response: response(
                headers: ["Content-Length": String(Wire.bodyLimit + 1)], body: Data([32])),
            finishResponse: false)
        defer { server.stop() }
        let transport = try authorizedTransport(server)

        do {
            _ = try await withinDeadline {
                try await transport.post(method: "pull", body: Data("{}".utf8))
            }
            XCTFail("Accepted a declared body over the limit")
        } catch {
            XCTAssertEqual(
                error as? SyncError, .invalid("body_limit"), "Unexpected error: \(error)")
        }
    }

    func testDecodedChunksCannotExceedLimitDespiteSmallCompressedLength() async throws {
        let compressed = try gzip(Data(repeating: 32, count: Wire.bodyLimit + 1))
        XCTAssertLessThan(compressed.count, 128 * 1024)
        let server = try await LoopbackHTTP.start(
            response: response(headers: ["Content-Encoding": "gzip"], body: compressed))
        defer { server.stop() }
        let transport = try authorizedTransport(server)

        do {
            _ = try await withinDeadline(seconds: 10) {
                try await transport.post(method: "snapshot", body: Data("{}".utf8))
            }
            XCTFail("Applied the limit to compressed bytes instead of decoded chunks")
        } catch {
            XCTAssertEqual(error as? SyncError, .invalid("body_limit"))
        }
    }

    func testInterruptedBodyIsTransientInsteadOfACompleteMalformedReply() async throws {
        let server = try await LoopbackHTTP.start(
            response: response(headers: ["Content-Length": "100"], body: Data("{}".utf8)))
        defer { server.stop() }
        let transport = try authorizedTransport(server)

        do {
            _ = try await withinDeadline {
                try await transport.post(method: "pull", body: Data("{}".utf8))
            }
            XCTFail("Returned a successful partial body")
        } catch {
            XCTAssertEqual(error as? SyncError, .transient(retryAfter: nil))
        }
    }

    func testProxyFailuresPreserveRetryAfterWithoutWaitingForOrParsingTheirBody() async throws {
        for status in ["429 Too Many Requests", "502 Bad Gateway", "503 Service Unavailable"] {
            let server = try await LoopbackHTTP.start(
                response: response(
                    status: status,
                    headers: ["Retry-After": "900", "Content-Length": "1000000000"],
                    body: Data("proxy text, not JSON".utf8)),
                finishResponse: false)
            defer { server.stop() }
            let transport = try authorizedTransport(server)

            let reply = try await withinDeadline {
                try await transport.post(method: "push", body: Data("{}".utf8))
            }

            XCTAssertEqual(reply.headers["retry-after"], "900")
            XCTAssertTrue(reply.body.isEmpty)
            XCTAssertThrowsError(try reply.success()) { error in
                XCTAssertEqual(error as? SyncError, .transient(retryAfter: 900))
            }
        }
    }

    func testCancellationEndsAStalledExchange() async throws {
        let server = try await LoopbackHTTP.start(
            response: response(headers: ["Content-Length": "100"]), finishResponse: false)
        defer { server.stop() }
        let transport = try authorizedTransport(server)
        let request = Task {
            try await transport.post(method: "pull", body: Data("{}".utf8))
        }
        defer { request.cancel() }
        try await withinDeadline { try await server.waitForRequest() }

        request.cancel()

        do {
            _ = try await withinDeadline { try await request.value }
            XCTFail("Cancellation returned a response")
        } catch {
            XCTAssertTrue(error is CancellationError, "Unexpected cancellation error: \(error)")
        }
    }

    func testRedirectIsNotFollowedAndDoesNotForwardAuthorization() async throws {
        let destination = try await LoopbackHTTP.start(response: response(body: Data("{}".utf8)))
        defer { destination.stop() }
        let origin = try await LoopbackHTTP.start(
            response: response(
                status: "302 Found",
                headers: [
                    "Location": destination.endpoint.appendingPathComponent("capture")
                        .absoluteString
                ]))
        defer { origin.stop() }
        let transport = try authorizedTransport(origin)

        let reply = try await withinDeadline {
            try await transport.post(method: "pull", body: Data("{}".utf8))
        }

        XCTAssertEqual(reply.status, 302)
        XCTAssertEqual(origin.requests.count, 1)
        XCTAssertTrue(origin.requests[0].contains("Bearer transport-test-secret"))
        XCTAssertTrue(destination.requests.isEmpty, "The redirect target received a request")
        XCTAssertThrowsError(try reply.success())
    }

    private func authorizedTransport(_ server: LoopbackHTTP) throws -> URLSessionTransport {
        try URLSessionTransport(endpoint: server.endpoint) {
            ["Authorization": "Bearer transport-test-secret"]
        }
    }
}

private enum TransportFixtureError: Error {
    case timeout, listenerStopped
    case compression(Int32)
}

private func withinDeadline<Value: Sendable>(
    seconds: Double = 5,
    _ operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
    try await withThrowingTaskGroup(of: Value.self) { tasks in
        tasks.addTask(operation: operation)
        tasks.addTask {
            try await Task.sleep(for: .seconds(seconds))
            throw TransportFixtureError.timeout
        }
        defer { tasks.cancelAll() }
        return try await tasks.next()!
    }
}

private func response(
    status: String = "200 OK", headers: [String: String] = [:], body: Data = Data()
) -> Data {
    let fields = [
        "Content-Type": "application/json", "Content-Length": String(body.count),
        "Connection": "close",
    ].merging(headers) { _, value in value }
    let lines = ["HTTP/1.1 \(status)"] + fields.keys.sorted().map { "\($0): \(fields[$0]!)" }
    return Data((lines.joined(separator: "\r\n") + "\r\n\r\n").utf8) + body
}

/// zlib creates a real gzip fixture; URLSession performs the decompression.
private func gzip(_ data: Data) throws -> Data {
    var stream = z_stream()
    let initialized = deflateInit2_(
        &stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8,
        Z_DEFAULT_STRATEGY, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size))
    guard initialized == Z_OK else { throw TransportFixtureError.compression(initialized) }
    defer { deflateEnd(&stream) }

    var output = Data(count: Int(compressBound(uLong(data.count))) + 32)
    let status = data.withUnsafeBytes { input in
        output.withUnsafeMutableBytes { buffer in
            stream.next_in = UnsafeMutablePointer(
                mutating: input.bindMemory(to: Bytef.self).baseAddress)
            stream.avail_in = uInt(input.count)
            stream.next_out = buffer.bindMemory(to: Bytef.self).baseAddress
            stream.avail_out = uInt(buffer.count)
            return deflate(&stream, Z_FINISH)
        }
    }
    guard status == Z_STREAM_END else { throw TransportFixtureError.compression(status) }
    return Data(output.prefix(Int(stream.total_out)))
}

/// A local byte fixture, not an HTTP implementation: wait for the request's
/// header terminator, record it, and send one predetermined response. A final
/// send can intentionally close a truncated body or leave the peer waiting.
private final class LoopbackHTTP: @unchecked Sendable {
    private let listener: NWListener
    private let queue = DispatchQueue(label: "ReplicamanSync.TransportTests.loopback")
    private let lock = NSLock()
    private let response: Data
    private let finishResponse: Bool
    private var ready: CheckedContinuation<URL, any Error>?
    private var connections: [NWConnection] = []
    private var captured: [String] = []
    private var address: URL?

    var endpoint: URL { lock.withLock { address! } }
    var requests: [String] { lock.withLock { captured } }

    private init(response: Data, finishResponse: Bool) throws {
        let parameters = NWParameters.tcp
        parameters.requiredLocalEndpoint = .hostPort(host: .ipv4(.loopback), port: .any)
        listener = try NWListener(using: parameters)
        self.response = response
        self.finishResponse = finishResponse
    }

    static func start(response: Data, finishResponse: Bool = true) async throws -> LoopbackHTTP {
        let server = try LoopbackHTTP(response: response, finishResponse: finishResponse)
        do {
            _ = try await withinDeadline { try await server.listen() }
            return server
        } catch {
            server.stop()
            throw error
        }
    }

    private func listen() async throws -> URL {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                lock.withLock { ready = continuation }
                listener.stateUpdateHandler = { [weak self] state in
                    guard let self else { return }
                    switch state {
                    case .ready:
                        guard let port = listener.port else { return }
                        completeStart(.success(URL(string: "http://127.0.0.1:\(port.rawValue)")!))
                    case .failed(let error): completeStart(.failure(error))
                    case .cancelled: completeStart(.failure(TransportFixtureError.listenerStopped))
                    default: break
                    }
                }
                listener.newConnectionHandler = { [weak self] connection in
                    guard let self else {
                        connection.cancel()
                        return
                    }
                    lock.withLock { connections.append(connection) }
                    connection.start(queue: queue)
                    receiveHeaders(connection, previous: Data())
                }
                listener.start(queue: queue)
            }
        } onCancel: {
            self.stop()
        }
    }

    private func completeStart(_ result: Result<URL, any Error>) {
        let continuation = lock.withLock {
            if case .success(let url) = result { address = url }
            defer { ready = nil }
            return ready
        }
        continuation?.resume(with: result)
    }

    private func receiveHeaders(_ connection: NWConnection, previous: Data) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) {
            [weak self] data, _, complete, error in
            guard let self else {
                connection.cancel()
                return
            }
            var received = previous
            if let data { received.append(data) }
            if let terminator = received.range(of: Data("\r\n\r\n".utf8)) {
                let headers = String(decoding: received[..<terminator.upperBound], as: UTF8.self)
                lock.withLock { captured.append(headers) }
                connection.send(
                    content: response,
                    contentContext: finishResponse ? .finalMessage : .defaultMessage,
                    isComplete: true,
                    completion: .contentProcessed { _ in })
            } else if !complete && error == nil && received.count < 64 * 1024 {
                receiveHeaders(connection, previous: received)
            } else {
                connection.cancel()
            }
        }
    }

    func waitForRequest() async throws {
        while requests.isEmpty { try await Task.sleep(for: .milliseconds(10)) }
    }

    func stop() {
        listener.cancel()
        let active = lock.withLock {
            defer { connections.removeAll() }
            return connections
        }
        for connection in active { connection.cancel() }
    }
}
