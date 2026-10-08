import Foundation

/// JSON whose objects were checked for duplicate *decoded* keys before insertion.
/// Numbers retain their spelling; message headers accept only signed 32-bit integers.
public indirect enum JSON: Equatable, Sendable, Codable {
    case null
    case bool(Bool)
    case number(String)
    case string(String)
    case array([JSON])
    case object([String: JSON])

    public static func parse(_ data: Data, limit: Int = Wire.bodyLimit) throws -> JSON {
        guard data.count <= limit else { throw SyncError.invalid("body_limit") }
        guard String(data: data, encoding: .utf8) != nil else { throw SyncError.invalid("utf8") }
        var parser = Parser(bytes: Array(data))
        let value = try parser.value(depth: 0)
        parser.space()
        guard parser.index == parser.bytes.count else { throw SyncError.invalid("trailing_json") }
        return value
    }

    public func encoded() -> Data { Data(text.utf8) }
    public var text: String {
        switch self {
        case .null: return "null"
        case .bool(let value): return value ? "true" : "false"
        case .number(let value): return value
        case .string(let value): return Self.quote(value)
        case .array(let values): return "[" + values.map(\.text).joined(separator: ",") + "]"
        case .object(let values):
            return "{"
                + values.keys.sorted { $0.utf16.lexicographicallyPrecedes($1.utf16) }
                .map { Self.quote($0) + ":" + values[$0]!.text }.joined(separator: ",") + "}"
        }
    }

    static func quote(_ value: String) -> String {
        var result = "\""
        for scalar in value.unicodeScalars {
            switch scalar.value {
            case 0x22: result += "\\\""
            case 0x5c: result += "\\\\"
            case 8: result += "\\b"
            case 9: result += "\\t"
            case 10: result += "\\n"
            case 12: result += "\\f"
            case 13: result += "\\r"
            case 0..<32: result += String(format: "\\u%04x", scalar.value)
            default: result.unicodeScalars.append(scalar)
            }
        }
        return result + "\""
    }

    public var object: [String: JSON]? { if case .object(let value) = self { value } else { nil } }
    public var array: [JSON]? { if case .array(let value) = self { value } else { nil } }
    public var string: String? { if case .string(let value) = self { value } else { nil } }
    var bool: Bool? { if case .bool(let value) = self { value } else { nil } }
    var integer: Int64? {
        guard case .number(let value) = self,
            value.range(of: #"^-?(0|[1-9][0-9]*)$"#, options: .regularExpression) != nil
        else { return nil }
        return Int64(value)
    }

    private struct Parser {
        let bytes: [UInt8]
        var index = 0
        mutating func space() {
            while index < bytes.count && [9, 10, 13, 32].contains(bytes[index]) { index += 1 }
        }
        mutating func take(_ byte: UInt8) -> Bool {
            guard index < bytes.count, bytes[index] == byte else { return false }
            index += 1
            return true
        }
        mutating func require(_ byte: UInt8) throws {
            guard take(byte) else { throw SyncError.invalid("json") }
        }
        mutating func value(depth: Int) throws -> JSON {
            guard depth <= 128 else { throw SyncError.invalid("json_depth") }
            space()
            guard index < bytes.count else { throw SyncError.invalid("json") }
            switch bytes[index] {
            case 34: return .string(try string())
            case 123:
                index += 1
                space()
                var result: [String: JSON] = [:]
                if take(125) { return .object(result) }
                repeat {
                    space()
                    let key = try string()
                    guard result[key] == nil else { throw SyncError.invalid("duplicate_key") }
                    space()
                    try require(58)
                    result[key] = try value(depth: depth + 1)
                    space()
                    if take(125) { return .object(result) }
                    try require(44)
                } while true
            case 91:
                index += 1
                space()
                var result: [JSON] = []
                if take(93) { return .array(result) }
                repeat {
                    result.append(try value(depth: depth + 1))
                    space()
                    if take(93) { return .array(result) }
                    try require(44)
                } while true
            case 116:
                try literal("true")
                return .bool(true)
            case 102:
                try literal("false")
                return .bool(false)
            case 110:
                try literal("null")
                return .null
            default:
                let start = index
                while index < bytes.count && ![9, 10, 13, 32, 44, 93, 125].contains(bytes[index]) {
                    index += 1
                }
                let token = String(decoding: bytes[start..<index], as: UTF8.self)
                guard
                    token.range(
                        of: #"^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$"#,
                        options: .regularExpression) != nil,
                    let value = Double(token), value.isFinite
                else { throw SyncError.invalid("number") }
                return .number(token)
            }
        }
        mutating func literal(_ text: String) throws {
            for byte in text.utf8 { try require(byte) }
        }
        mutating func hex() throws -> UInt32 {
            var result: UInt32 = 0
            for _ in 0..<4 {
                guard index < bytes.count else { throw SyncError.invalid("unicode") }
                let b = bytes[index]
                index += 1
                let digit: UInt32
                switch b {
                case 48...57: digit = UInt32(b - 48)
                case 65...70: digit = UInt32(b - 55)
                case 97...102: digit = UInt32(b - 87)
                default: throw SyncError.invalid("unicode")
                }
                result = result * 16 + digit
            }
            return result
        }
        mutating func string() throws -> String {
            try require(34)
            var result: [UInt8] = []
            while index < bytes.count {
                let byte = bytes[index]
                index += 1
                if byte == 34 {
                    guard let text = String(bytes: result, encoding: .utf8) else {
                        throw SyncError.invalid("utf8")
                    }
                    return text
                }
                guard byte >= 32 else { throw SyncError.invalid("string_control") }
                if byte != 92 {
                    result.append(byte)
                    continue
                }
                guard index < bytes.count else { throw SyncError.invalid("escape") }
                let escape = bytes[index]
                index += 1
                switch escape {
                case 34, 47, 92: result.append(escape)
                case 98: result.append(8)
                case 102: result.append(12)
                case 110: result.append(10)
                case 114: result.append(13)
                case 116: result.append(9)
                case 117:
                    var scalar = try hex()
                    if (0xd800...0xdbff).contains(scalar) {
                        try require(92)
                        try require(117)
                        let low = try hex()
                        guard (0xdc00...0xdfff).contains(low) else {
                            throw SyncError.invalid("surrogate")
                        }
                        scalar = 0x10000 + (scalar - 0xd800) * 1024 + (low - 0xdc00)
                    }
                    guard let decoded = UnicodeScalar(scalar) else {
                        throw SyncError.invalid("surrogate")
                    }
                    result.append(contentsOf: String(decoded).utf8)
                default: throw SyncError.invalid("escape")
                }
            }
            throw SyncError.invalid("string_end")
        }
    }
}

extension Dictionary where Key == String, Value == JSON {
    func field(_ name: String) throws -> JSON {
        guard let value = self[name] else { throw SyncError.invalid("missing_" + name) }
        return value
    }
    func string(_ name: String) throws -> String {
        guard let value = try field(name).string else { throw SyncError.invalid(name) }
        return value
    }
    func array(_ name: String) throws -> [JSON] {
        guard let value = try field(name).array else { throw SyncError.invalid(name) }
        return value
    }
}
