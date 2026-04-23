import Foundation

/// A struct representing a geographic location with latitude and longitude.
struct GeoLocation: Codable, Hashable {
    let latitude: Double
    let longitude: Double
}

/// A wrapper struct that makes GeoLocation conform to Identifiable for SwiftUI lists.
struct IdentifiableGeoLocation: Identifiable {
    /// A unique identifier for the GeoLocation instance.
    let id = UUID()
    
    /// The associated GeoLocation data.
    let geoLocation: GeoLocation
}
