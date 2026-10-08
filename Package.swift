// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "ReplicamanSync",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [.library(name: "ReplicamanSync", targets: ["ReplicamanSync"])],
    targets: [
        .target(name: "ReplicamanSync"),
        .testTarget(
            name: "ReplicamanSyncTests", dependencies: ["ReplicamanSync"],
            resources: [.copy("Fixtures")]),
    ]
)
