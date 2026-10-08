import CryptoKit
import Foundation
import Network

public enum SyncError: Error, Equatable, Sendable {
    case invalid(String)
    case server(code: String, status: Int, epoch: String?)
    case transient(retryAfter: TimeInterval?)
    case storage(String)
    case inapplicable
    case unsupportedFormat
    case busy
    case superseded
}

public enum Wire {
    public static let bodyLimit = 48 * 1024 * 1024
    public static let dataLimit = 32 * 1024 * 1024
    public static let pageLimit = 1024 * 1024
    public static func uuid() -> String { UUID().uuidString.lowercased() }

    static func check(_ condition: Bool, _ name: String) throws {
        guard condition else { throw SyncError.invalid(name) }
    }
    static func matches(_ text: String, _ pattern: String) -> Bool {
        guard let range = text.range(of: pattern, options: .regularExpression) else { return false }
        return !(pattern.hasPrefix("^") && pattern.hasSuffix("$"))
            || range == text.startIndex..<text.endIndex
    }
    static func uuid(_ text: String) throws {
        try check(
            matches(
                text, #"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"#),
            "uuid")
    }
    static func hash(_ text: String) throws {
        try check(matches(text, #"^[0-9a-f]{64}$"#), "hash")
    }
    static func code(_ text: String) throws {
        try check(matches(text, #"^[a-z][a-z0-9_]{0,63}$"#), "code")
    }
    static func position(_ value: JSON) throws -> Int64 {
        guard let text = value.string, matches(text, #"^(0|[1-9][0-9]{0,18})$"#),
            let result = Int64(text)
        else { throw SyncError.invalid("position") }
        return result
    }
    static func object(_ value: JSON) throws -> [String: JSON] {
        guard let object = value.object else { throw SyncError.invalid("object") }
        return object
    }
    static func token(_ value: JSON) throws -> String? {
        if value == .null { return nil }
        guard let text = value.string, (1...2048).contains(text.utf8.count),
            text.utf8.allSatisfy({ $0 < 128 })
        else {
            throw SyncError.invalid("page_token")
        }
        return text
    }
    static func base64(_ value: String) throws -> Data {
        try check(value.utf8.count <= ((dataLimit + 2) / 3) * 4, "data_limit")
        guard let data = Data(base64Encoded: value), data.count <= dataLimit,
            data.base64EncodedString() == value
        else {
            throw SyncError.invalid("base64")
        }
        return data
    }
    static func context(_ object: [String: JSON], _ scope: Scope) throws {
        try check(
            try object.string("stream") == scope.stream && object.string("epoch") == scope.epoch,
            "context")
    }
    static func uri(_ value: String, absolute: Bool = false) -> Bool {
        guard
            !value.isEmpty,
            value.utf8.allSatisfy({ $0 < 128 }),
            let components = URLComponents(string: value, encodingInvalidCharacters: false),
            validIPLiteral(in: components)
        else {
            return false
        }

        return !absolute || (components.scheme != nil && components.fragment == nil)
    }

    private static func validIPLiteral(in components: URLComponents) -> Bool {
        // Foundation checks URI syntax but also accepts bracketed non-addresses.
        guard let host = components.encodedHost, host.hasPrefix("[") else { return true }
        guard host.hasSuffix("]") else { return false }

        let address = String(host.dropFirst().dropLast())
        if !address.contains("%"), IPv6Address(address) != nil { return true }

        return address.wholeMatch(of: #/[vV][0-9A-Fa-f]+\.[A-Za-z0-9._~!$&'()*+,;=:-]+/#) != nil
    }
    static func timestamp(_ text: String) -> Bool {
        guard
            matches(
                text,
                #"^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})$"#
            )
        else { return false }
        let bytes = Array(text.utf8)
        func number(_ start: Int, _ count: Int) -> Int {
            Int(String(decoding: bytes[start..<(start + count)], as: UTF8.self))!
        }
        let year = number(0, 4)
        let month = number(5, 2)
        let day = number(8, 2)
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
        let days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        guard (1...12).contains(month), (1...days[month - 1]).contains(day), number(11, 2) < 24,
            number(14, 2) < 60, number(17, 2) <= 60
        else { return false }
        return bytes.last == 90 || bytes.last == 122
            || (number(bytes.count - 5, 2) < 24 && number(bytes.count - 2, 2) < 60)
    }
}

public struct Scope: Codable, Equatable, Sendable {
    public let subject: String
    public let stream: String
    public let epoch: String
    public let client: String
    public init(subject: String, stream: String, epoch: String, client: String) throws {
        try Wire.check(!subject.isEmpty, "subject")
        try Wire.check(Wire.matches(stream, #"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$"#), "stream")
        try Wire.uuid(epoch)
        try Wire.uuid(client)
        self.subject = subject
        self.stream = stream
        self.epoch = epoch
        self.client = client
    }
    var fields: [String: JSON] { ["stream": .string(stream), "epoch": .string(epoch)] }
}

public struct CloudEvent: Equatable, Sendable, Codable {
    public let header: [String: JSON]
    public let data: Data
    public var id: String { header["id"]!.string! }
    public var type: String { header["type"]!.string! }
    public var json: JSON {
        .object(
            header.merging(["data_base64": .string(data.base64EncodedString())]) { _, new in new })
    }
    public var hash: String {
        var digest = SHA256()
        digest.update(data: Data("replicaman-message/1\0".utf8))
        digest.update(data: JSON.object(header).encoded())
        digest.update(data: Data([0]))
        digest.update(data: data)
        return digest.finalize().map { String(format: "%02x", $0) }.joined()
    }

    public init(
        id: String = Wire.uuid(), source: String, type: String, contentType: String,
        data: Data, attributes: [String: JSON] = [:]
    ) throws {
        try Wire.check(
            attributes.keys.allSatisfy {
                !["specversion", "id", "source", "type", "datacontenttype", "data_base64"].contains(
                    $0)
            }, "reserved_attribute")
        var fields = attributes
        fields["specversion"] = .string("1.0")
        fields["id"] = .string(id)
        fields["source"] = .string(source)
        fields["type"] = .string(type)
        fields["datacontenttype"] = .string(contentType)
        fields["data_base64"] = .string(data.base64EncodedString())
        try self.init(json: .object(fields))
    }

    public init(json: JSON) throws {
        var fields = try Wire.object(json)
        let standards = [
            "specversion", "id", "source", "type", "datacontenttype", "data_base64", "subject",
            "time", "dataschema",
        ]
        for (key, value) in fields {
            try Wire.check(value != .null, "null_attribute")
            if let text = value.string {
                try Wire.check(
                    text.unicodeScalars.allSatisfy {
                        let value = $0.value
                        return value >= 32 && !(127...159).contains(value)
                            && !(0xfdd0...0xfdef).contains(value)
                            && value & 0xffff != 0xfffe && value & 0xffff != 0xffff
                    }, "attribute_control")
            }
            if standards.contains(key) {
                try Wire.check(value.string != nil, "attribute_type")
            } else {
                try Wire.check(
                    key != "data" && Wire.matches(key, #"^[a-z][a-z0-9]{0,19}$"#), "attribute_name")
                switch value {
                case .string, .bool: break
                case .number:
                    guard let number = value.integer, Int32(exactly: number) != nil else {
                        throw SyncError.invalid("attribute_number")
                    }
                    fields[key] = .number(String(number))
                default: throw SyncError.invalid("attribute_type")
                }
            }
        }
        guard try fields.string("specversion") == "1.0" else { throw SyncError.unsupportedFormat }
        try Wire.uuid(fields.string("id"))
        let source = try fields.string("source")
        try Wire.check(source.utf8.count <= 1024 && Wire.uri(source), "source")
        let type = try fields.string("type")
        try Wire.check(!type.isEmpty && type.utf8.count <= 255, "type")
        let media = try fields.string("datacontenttype")
        let token = #"[!#$%&'*+.^_`|~0-9A-Za-z-]+"#
        try Wire.check(
            media.utf8.count <= 255 && media.utf8.allSatisfy { $0 < 128 }
                && Wire.matches(
                    media,
                    "^" + token + "/" + token + #"(?:\s*;\s*"# + token + "=(?:" + token
                        + #"|\"(?:[^\"\\\r\n]|\\[^\r\n])*\"))*$"#),
            "content_type")
        try Wire.check(
            !media.hasPrefix("*/") && !media.components(separatedBy: ";")[0].hasSuffix("/*"),
            "content_type")
        if let subject = fields["subject"]?.string { try Wire.check(!subject.isEmpty, "subject") }
        if let schema = fields["dataschema"]?.string {
            try Wire.check(Wire.uri(schema, absolute: true), "dataschema")
        }
        if let time = fields["time"]?.string {
            try Wire.check(Wire.timestamp(time), "time")
        }
        self.data = try Wire.base64(fields.string("data_base64"))
        fields.removeValue(forKey: "data_base64")
        try Wire.check(JSON.object(fields).encoded().count <= 8192, "header_limit")
        self.header = fields
    }

    public init(from decoder: any Decoder) throws {
        let box = try decoder.container(keyedBy: CodingKeys.self)
        let header = try box.decode([String: JSON].self, forKey: .header)
        let data = try box.decode(Data.self, forKey: .data)
        try self.init(
            json: .object(
                header.merging(["data_base64": .string(data.base64EncodedString())]) { _, rhs in rhs
                }))
    }
    private enum CodingKeys: String, CodingKey { case header, data }
}

public enum Outcome: Equatable, Sendable, Codable {
    case accepted(cursor: Int64, reason: String?)  // nil means applied; a reason means noop.
    case rejected(reason: String)

    var cursor: Int64? { if case .accepted(let cursor, _) = self { cursor } else { nil } }
    static func parse(_ fields: [String: JSON]) throws -> Outcome {
        switch try fields.string("status") {
        case "accepted":
            let position = try Wire.position(fields.field("cursor"))
            switch try fields.string("result") {
            case "applied":
                try Wire.check(fields["reason"] == nil, "applied_reason")
                return .accepted(cursor: position, reason: nil)
            case "noop":
                let reason = try fields.string("reason")
                try Wire.code(reason)
                return .accepted(cursor: position, reason: reason)
            default: throw SyncError.unsupportedFormat
            }
        case "rejected":
            try Wire.check(fields["cursor"] == nil && fields["result"] == nil, "rejected_fields")
            let reason = try fields.string("reason")
            try Wire.code(reason)
            return .rejected(reason: reason)
        default: throw SyncError.unsupportedFormat
        }
    }
}

public struct Receipt: Equatable, Sendable, Codable {
    public let op: String
    public let hash: String
    public let outcome: Outcome
}

public struct LogEvent: Equatable, Sendable {
    public let cursor: Int64
    public let op: String?
    public let hash: String?
    public let message: CloudEvent?
    public let reason: String?
    public var outcome: Outcome { .accepted(cursor: cursor, reason: reason) }

    init(json: JSON) throws {
        let fields = try Wire.object(json)
        cursor = try Wire.position(fields.field("cursor"))
        let opValue = try fields.field("op")
        let hashValue = try fields.field("hash")
        if opValue == .null && hashValue == .null {
            op = nil
            hash = nil
        } else {
            let id = try fields.string("op")
            let digest = try fields.string("hash")
            try Wire.uuid(id)
            try Wire.hash(digest)
            op = id
            hash = digest
        }
        let value = try fields.field("message")
        if value == .null {
            message = nil
            reason = try fields.string("reason")
            try Wire.code(reason!)
        } else {
            try Wire.check(fields["reason"] == nil, "change_reason")
            message = try CloudEvent(json: value)
            reason = nil
            if let op { try Wire.check(message?.id != op, "event_id") }
        }
    }
}

public struct PullPage: Equatable, Sendable {
    public let events: [LogEvent]
    public let cursor: Int64
    public let to: Int64
    public let head: Int64
    public let more: Bool

    init(data: Data, scope: Scope, after: Int64, to target: Int64?, limit: Int) throws {
        let fields = try Wire.object(JSON.parse(data))
        try Wire.context(fields, scope)
        cursor = try Wire.position(fields.field("cursor"))
        to = try Wire.position(fields.field("to"))
        head = try Wire.position(fields.field("head"))
        guard let more = try fields.field("more").bool else { throw SyncError.invalid("more") }
        self.more = more
        events = try fields.array("events").map(LogEvent.init)
        try Wire.check(after <= cursor && cursor <= to && to <= head, "positions")
        try Wire.check(target.map { $0 == to } ?? (to == head), "target")
        try Wire.check(
            events.count <= limit && (data.count <= Wire.pageLimit || events.count == 1),
            "page_limit")
        var previous = after
        for event in events {
            try Wire.check(previous < event.cursor && event.cursor <= cursor, "event_order")
            previous = event.cursor
        }
        try Wire.check(
            more ? (!events.isEmpty && cursor == previous && cursor < to) : cursor == to,
            "page_progress")
    }
}

public struct Pending: Equatable, Sendable, Codable {
    public let op: String
    public let hash: String
    var json: JSON { .object(["op": .string(op), "hash": .string(hash)]) }
}

public enum Coverage: Equatable, Sendable, Codable {
    case decided(Outcome)
    case unseen, mismatch
}
public struct OperationCoverage: Equatable, Sendable, Codable {
    public let pending: Pending
    public let coverage: Coverage
}

public struct SnapshotPage: Equatable, Sendable {
    public let snapshot: String
    public let cursor: Int64
    public let items: [CloudEvent]
    public let next: String?
    public let ttl: Int
    public let ops: [OperationCoverage]

    init(data: Data, scope: Scope, attempt: String, pending: [Pending], limit: Int) throws {
        let fields = try Wire.object(JSON.parse(data))
        try Wire.context(fields, scope)
        try Wire.check(
            try fields.string("client") == scope.client && fields.string("attempt") == attempt,
            "snapshot_context")
        snapshot = try fields.string("snapshot")
        try Wire.uuid(snapshot)
        cursor = try Wire.position(fields.field("cursor"))
        items = try fields.array("items").map(CloudEvent.init)
        next = try Wire.token(fields.field("next"))
        guard let ttl = try fields.field("ttl").integer, (1...600).contains(ttl) else {
            throw SyncError.invalid("ttl")
        }
        self.ttl = Int(ttl)
        try Wire.check(
            items.count <= limit && (data.count <= Wire.pageLimit || items.count == 1), "page_limit"
        )
        try Wire.check(next == nil || !items.isEmpty, "empty_snapshot_page")
        let values = try fields.array("ops")
        try Wire.check(values.count == pending.count, "coverage_count")
        ops = try zip(values, pending).map { value, expected in
            let record = try Wire.object(value)
            try Wire.check(
                try record.string("op") == expected.op && record.string("hash") == expected.hash,
                "coverage_identity")
            let status = try record.string("status")
            let coverage: Coverage
            if status == "unseen" || status == "mismatch" {
                try Wire.check(
                    record["cursor"] == nil && record["result"] == nil && record["reason"] == nil,
                    "coverage_fields")
                coverage = status == "unseen" ? .unseen : .mismatch
            } else {
                coverage = .decided(try Outcome.parse(record))
            }
            return OperationCoverage(pending: expected, coverage: coverage)
        }
    }
}
