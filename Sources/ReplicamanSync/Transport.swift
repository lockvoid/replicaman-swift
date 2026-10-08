import Foundation

public struct HTTPReply: Sendable {
    public let status: Int
    public let headers: [String: String]
    public let body: Data
    public init(status: Int, headers: [String: String] = [:], body: Data) {
        self.status = status
        self.headers = headers.reduce(into: [:]) { $0[$1.key.lowercased()] = $1.value }
        self.body = body
    }

    func success() throws -> Data {
        if status == 408 || status == 429 || status >= 500 {
            var delay: TimeInterval?
            if let header = headers["retry-after"] {
                if let seconds = Double(header), seconds.isFinite, seconds >= 0 {
                    delay = seconds
                } else {
                    let formatter = DateFormatter()
                    formatter.locale = Locale(identifier: "en_US_POSIX")
                    formatter.timeZone = TimeZone(secondsFromGMT: 0)
                    formatter.dateFormat = "EEE',' dd MMM yyyy HH':'mm':'ss z"
                    delay = formatter.date(from: header).map { max(0, $0.timeIntervalSinceNow) }
                }
            }
            throw SyncError.transient(retryAfter: delay)
        }
        if status == 200 { return body }
        let object = try Wire.object(JSON.parse(body))
        let code = try object.string("error")
        try Wire.code(code)
        let allowed: [Int: Set<String>] = [
            400: ["bad_request", "unsupported_protocol"], 401: ["unauthorized"],
            403: ["stream_forbidden", "write_forbidden"], 404: ["stream_not_found"],
            410: ["stream_gone"],
            409: [
                "epoch_changed", "cursor_expired", "snapshot_expired", "cursor_invalid",
                "attempt_changed", "operation_changed",
            ],
            413: ["payload_too_large"], 415: ["unsupported_media_type"],
            422: ["unsupported_format"],
        ]
        try Wire.check(allowed[status]?.contains(code) == true, "http_status")
        let epoch = code == "epoch_changed" ? try object.string("epoch") : nil
        if let epoch { try Wire.uuid(epoch) }
        throw SyncError.server(code: code, status: status, epoch: epoch)
    }
}

public protocol SyncTransport: Sendable {
    /// Must bound the whole exchange, including response reads, to 60 seconds.
    /// Returning a complete malformed body differs from throwing a lost exchange.
    func post(method: String, body: Data) async throws -> HTTPReply
}

/// HTTP only; credentials and discovery remain application responsibilities.
/// Headers are refreshed per request. Redirects never forward credentials.
public struct URLSessionTransport: SyncTransport {
    private let endpoint: URL
    private let headers: @Sendable () async throws -> [String: String]

    public init(endpoint: URL, headers: @escaping @Sendable () async throws -> [String: String])
        throws
    {
        try Wire.check(
            endpoint.scheme == "https"
                || (endpoint.scheme == "http"
                    && ["127.0.0.1", "localhost", "::1"].contains(endpoint.host)),
            "https_required")
        self.endpoint = endpoint
        self.headers = headers
    }

    public func post(method: String, body: Data) async throws -> HTTPReply {
        try Wire.check(["push", "pull", "snapshot"].contains(method), "method")
        try Wire.check(body.count <= Wire.bodyLimit, "body_limit")
        var request = URLRequest(url: endpoint.appendingPathComponent("replicaman/v1/" + method))
        request.httpMethod = "POST"
        request.httpBody = body
        request.timeoutInterval = 60
        for (key, value) in try await headers() { request.setValue(value, forHTTPHeaderField: key) }
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.setValue("no-store", forHTTPHeaderField: "Cache-Control")
        return try await Exchange().send(request)
    }
}

/// Delegate chunks are bounded before appending. URLSession's resource timeout
/// covers the entire exchange, unlike a per-chunk inactivity timeout.
private final class Exchange: NSObject, URLSessionDataDelegate, @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<HTTPReply, any Error>?
    private var result: Result<HTTPReply, any Error>?
    private var session: URLSession?
    private var task: URLSessionDataTask?
    private var response: HTTPURLResponse?
    private var data = Data()

    func send(_ request: URLRequest) async throws -> HTTPReply {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                lock.lock()
                if let result {
                    lock.unlock()
                    continuation.resume(with: result)
                    return
                }
                self.continuation = continuation
                let config = URLSessionConfiguration.ephemeral
                config.timeoutIntervalForRequest = 60
                config.timeoutIntervalForResource = 60
                config.urlCache = nil
                config.httpCookieStorage = nil
                config.requestCachePolicy = .reloadIgnoringLocalCacheData
                let session = URLSession(configuration: config, delegate: self, delegateQueue: nil)
                let task = session.dataTask(with: request)
                self.session = session
                self.task = task
                lock.unlock()
                task.resume()
            }
        } onCancel: {
            self.finish(.failure(CancellationError()))
        }
    }

    private func finish(_ result: Result<HTTPReply, any Error>) {
        lock.lock()
        guard self.result == nil else {
            lock.unlock()
            return
        }
        self.result = result
        let continuation = self.continuation
        self.continuation = nil
        let task = self.task
        self.task = nil
        let session = self.session
        self.session = nil
        data = Data()
        lock.unlock()
        task?.cancel()
        session?.invalidateAndCancel()
        continuation?.resume(with: result)
    }

    private func reply(_ response: HTTPURLResponse, body: Data) -> HTTPReply {
        let headers = response.allHeaderFields.reduce(into: [String: String]()) { values, pair in
            values[String(describing: pair.key).lowercased()] = String(describing: pair.value)
        }
        return HTTPReply(status: response.statusCode, headers: headers, body: body)
    }

    func urlSession(
        _ session: URLSession, dataTask: URLSessionDataTask,
        didReceive response: URLResponse,
        completionHandler: @escaping (URLSession.ResponseDisposition) -> Void
    ) {
        guard let response = response as? HTTPURLResponse else {
            completionHandler(.cancel)
            finish(.failure(SyncError.invalid("http_response")))
            return
        }
        if response.statusCode == 408 || response.statusCode == 429 || response.statusCode >= 500 {
            completionHandler(.cancel)
            finish(.success(reply(response, body: Data())))
            return
        }
        if response.expectedContentLength > Wire.bodyLimit {
            completionHandler(.cancel)
            finish(.failure(SyncError.invalid("body_limit")))
            return
        }
        lock.lock()
        self.response = response
        lock.unlock()
        completionHandler(.allow)
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive chunk: Data) {
        lock.lock()
        guard result == nil else {
            lock.unlock()
            return
        }
        if chunk.count > Wire.bodyLimit - data.count {
            lock.unlock()
            finish(.failure(SyncError.invalid("body_limit")))
            return
        }
        data.append(chunk)
        lock.unlock()
    }

    func urlSession(
        _ session: URLSession, task: URLSessionTask, didCompleteWithError error: (any Error)?
    ) {
        lock.lock()
        let response = response
        let body = data
        lock.unlock()
        if let error {
            if error is CancellationError || (error as? URLError)?.code == .cancelled {
                finish(.failure(CancellationError()))
            } else {
                finish(.failure(SyncError.transient(retryAfter: nil)))
            }
        } else if let response {
            finish(.success(reply(response, body: body)))
        } else {
            finish(.failure(SyncError.transient(retryAfter: nil)))
        }
    }

    func urlSession(
        _ session: URLSession, task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}
