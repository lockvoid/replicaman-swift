import Foundation
import XCTest

@testable import ReplicamanSync

final class URIValidationTests: XCTestCase {
    func testSourcesAcceptAbsoluteAndRelativeReferencesWithoutRewritingThem() throws {
        let references = [
            "urn:ferment:client:example",
            "https://example.org/path?key=value#fragment",
            "/catalog/1",
            "../catalog/1",
            "?query",
            "#fragment",
            "https://example.org/a%2Fb",
            "https://%5Bnot-ip%5D/",
            "https://[::1]:443/path",
            "custom://[v1.a:b]/path",
        ]

        for reference in references {
            let event = try CloudEvent(
                source: reference,
                type: "example.value.v1",
                contentType: "text/plain",
                data: Data()
            )

            XCTAssertEqual(event.header["source"], .string(reference))
        }
    }

    func testSourcesRejectInvalidReferencesInsteadOfEncodingOrRepairingThem() {
        let references = [
            "",
            "contains a space",
            "https://example.org/%",
            "https://example.org/%GG",
            "https://example.org/путь",
            "https://example.org/raw[bracket]",
            "https://example.org/back\\slash",
            "https://example.org/#one#two",
            "1scheme:value",
            "https://example.org:invalid/",
            "https://[not-ip]/",
            "https://[]/",
            "custom://[v.a]/",
            "https://[fe80::1%25en0]/",
        ]

        for reference in references {
            XCTAssertFalse(Wire.uri(reference), reference)
        }
    }

    func testDataSchemaRequiresAnAbsoluteURIWithoutAFragment() {
        XCTAssertTrue(Wire.uri("urn:example:schema", absolute: true))
        XCTAssertTrue(Wire.uri("https://example.org/schema", absolute: true))
        XCTAssertFalse(Wire.uri("/schema", absolute: true))
        XCTAssertFalse(Wire.uri("https://example.org/schema#part", absolute: true))
    }
}
