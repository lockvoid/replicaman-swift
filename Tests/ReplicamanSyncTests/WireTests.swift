import Foundation
import XCTest

@testable import ReplicamanSync

final class WireTests: XCTestCase {
    func testSharedHashAndBase64Fixtures() throws {
        let url = Bundle.module.url(
            forResource: "wire", withExtension: "json", subdirectory: "Fixtures")!
        let fixture = try Wire.object(JSON.parse(Data(contentsOf: url)))
        for value in try fixture.array("message_hash") {
            let item = try Wire.object(value)
            let event = try CloudEvent(json: item.field("message"))
            XCTAssertEqual(event.hash, try item.string("sha256"))
            XCTAssertEqual(JSON.object(event.header).text, try item.string("canonical_header"))
        }
        for value in try fixture.array("base64") {
            let item = try Wire.object(value)
            if item["valid"] == .bool(true) {
                XCTAssertNoThrow(try Wire.base64(item.string("encoded")))
            } else {
                XCTAssertThrowsError(try Wire.base64(item.string("encoded")))
            }
        }
    }

    func testUnescapedDuplicateAndUnicodeBeforeDictionaryConstruction() throws {
        let invalid = [
            #"{"x":1,"\u0078":2}"#, #"{"nested":{"x":1,"x":2}}"#,
            #""\ud800""#, #""\udc00""#, #""\ud800\u0000""#, #"[1e999]"#,
            #"[01]"#, #"[+1]"#, #"[1,]"#, #"{"x":true,}"#, "\"line\nbreak\"",
        ]
        for text in invalid { XCTAssertThrowsError(try JSON.parse(Data(text.utf8)), text) }
        XCTAssertThrowsError(try JSON.parse(Data([0x22, 0xff, 0x22])))
        XCTAssertEqual(try JSON.parse(Data(#""\ud83d\ude42""#.utf8)), .string("🙂"))
        XCTAssertEqual(
            try JSON.parse(Data(#"{"x":"\\\"\b\f\n\r\t/"}"#.utf8)).object?["x"]?.string,
            "\\\"\u{8}\u{c}\n\r\t/")
    }

    func testCloudEventRejectsAmbiguityAndNormalizesOuterJSONOnly() throws {
        let event = try message("payload")
        var fields = event.json.object!
        fields["subject"] = .null
        XCTAssertThrowsError(try CloudEvent(json: .object(fields)))
        fields = event.json.object!
        fields["data_base64"] = .string("AB==")
        XCTAssertThrowsError(try CloudEvent(json: .object(fields)))
        fields = event.json.object!
        fields["other"] = .number("2147483648")
        XCTAssertThrowsError(try CloudEvent(json: .object(fields)))
        fields = event.json.object!
        fields["id"] = .string(event.id.uppercased())
        XCTAssertThrowsError(try CloudEvent(json: .object(fields)))
        let reordered = try JSON.parse(Data(" \n".utf8) + event.json.encoded())
        XCTAssertEqual(try CloudEvent(json: reordered).hash, event.hash)
        XCTAssertNotEqual(try message(" payload", id: event.id).hash, event.hash)
    }

    func testPositionsAndPullValidation() throws {
        let scope = try testScope()
        for invalid in ["00", "-1", "9223372036854775808", "1.0", "1e1"] {
            XCTAssertThrowsError(try Wire.position(.string(invalid)))
        }
        XCTAssertEqual(try Wire.position(.string("9223372036854775807")), Int64.max)
        XCTAssertThrowsError(try Wire.position(.number("1")))
        var fields = pull(scope: scope, cursor: 2, to: 2, head: 3).object!
        XCTAssertThrowsError(
            try PullPage(
                data: JSON.object(fields).encoded(), scope: scope, after: 0, to: nil, limit: 100))
        XCTAssertNoThrow(
            try PullPage(
                data: JSON.object(fields).encoded(), scope: scope, after: 0, to: 2, limit: 100))
        fields["to"] = .string("3")
        fields["cursor"] = .string("3")
        XCTAssertNoThrow(
            try PullPage(
                data: JSON.object(fields).encoded(), scope: scope, after: 0, to: nil, limit: 100))
        fields["more"] = .bool(true)
        XCTAssertThrowsError(
            try PullPage(
                data: JSON.object(fields).encoded(), scope: scope, after: 0, to: nil, limit: 100))
    }

    func testRequiredNullableKeysAndReceiptVariants() throws {
        let scope = try testScope()
        var event = log(try message("a"), cursor: 1).object!
        event.removeValue(forKey: "op")
        XCTAssertThrowsError(try LogEvent(json: .object(event)))
        event = log(try message("a"), cursor: 1).object!
        event["message"] = .null
        XCTAssertThrowsError(try LogEvent(json: .object(event)))
        event["reason"] = .string("already_applied")
        XCTAssertEqual(try LogEvent(json: .object(event)).reason, "already_applied")
        let attempt = Wire.uuid()
        var image = snapshot(scope: scope, attempt: attempt, cursor: 0).object!
        image.removeValue(forKey: "next")
        XCTAssertThrowsError(
            try SnapshotPage(
                data: JSON.object(image).encoded(), scope: scope, attempt: attempt, pending: [],
                limit: 100))
        image["next"] = .null
        image["ttl"] = .number("0")
        XCTAssertThrowsError(
            try SnapshotPage(
                data: JSON.object(image).encoded(), scope: scope, attempt: attempt, pending: [],
                limit: 100))
        XCTAssertThrowsError(
            try Outcome.parse([
                "status": .string("rejected"), "reason": .string("no"), "cursor": .string("1"),
            ]))
    }

    func testTransientProxyErrorsAndCompleteMalformedRepliesDiffer() throws {
        for status in [408, 429, 500, 502, 503] {
            XCTAssertThrowsError(
                try HTTPReply(
                    status: status, headers: ["Retry-After": "900"], body: Data("proxy".utf8)
                ).success()
            ) { error in
                XCTAssertEqual(error as? SyncError, .transient(retryAfter: 900))
            }
        }
        XCTAssertThrowsError(try HTTPReply(status: 201, body: Data("{}".utf8)).success())
        XCTAssertThrowsError(
            try HTTPReply(status: 403, body: Data(#"{"error":"future_error"}"#.utf8)).success())
        XCTAssertThrowsError(
            try HTTPReply(status: 409, body: Data(#"{"error":"epoch_changed"}"#.utf8)).success())
    }
}

func testScope(subject: String = "user:1", epoch: String = "f4c05d55-70c7-41d9-9868-c0582ae3a020")
    throws -> Scope
{
    try Scope(
        subject: subject, stream: "catalog/1", epoch: epoch,
        client: "ee24fb8f-8b5a-4dda-a0dc-4c5d020b5038")
}
func message(_ text: String, id: String = Wire.uuid()) throws -> CloudEvent {
    try CloudEvent(
        id: id, source: "urn:example:test", type: "example.value.v1", contentType: "text/plain",
        data: Data(text.utf8))
}
func log(_ message: CloudEvent?, cursor: Int64, pending: Pending? = nil, reason: String? = nil)
    -> JSON
{
    var fields: [String: JSON] = [
        "cursor": .string(String(cursor)), "op": pending.map { .string($0.op) } ?? .null,
        "hash": pending.map { .string($0.hash) } ?? .null, "message": message?.json ?? .null,
    ]
    if let reason { fields["reason"] = .string(reason) }
    return .object(fields)
}
func pull(
    scope: Scope, cursor: Int64, to: Int64? = nil, head: Int64? = nil, more: Bool = false,
    events: [JSON] = []
) -> JSON {
    .object(
        scope.fields.merging([
            "cursor": .string(String(cursor)), "to": .string(String(to ?? cursor)),
            "head": .string(String(head ?? to ?? cursor)), "more": .bool(more),
            "events": .array(events),
        ]) { _, rhs in rhs })
}
func snapshot(
    scope: Scope, attempt: String, id: String = "1dc8e6e8-a38c-4f34-8980-e8a5cc6a4284",
    cursor: Int64,
    items: [CloudEvent] = [], next: String? = nil, ttl: Int = 600, ops: [JSON] = []
) -> JSON {
    .object(
        scope.fields.merging([
            "client": .string(scope.client), "attempt": .string(attempt), "snapshot": .string(id),
            "cursor": .string(String(cursor)), "items": .array(items.map(\.json)),
            "next": next.map(JSON.string) ?? .null, "ttl": .number(String(ttl)), "ops": .array(ops),
        ]) { _, rhs in rhs })
}
func verdict(op: String, status: String = "accepted", cursor: Int64 = 1, reason: String? = nil)
    -> [String: JSON]
{
    var fields: [String: JSON] = ["op": .string(op), "status": .string(status)]
    if status == "accepted" {
        fields["cursor"] = .string(String(cursor))
        fields["result"] = .string(reason == nil ? "applied" : "noop")
    }
    if let reason { fields["reason"] = .string(reason) }
    return fields
}
