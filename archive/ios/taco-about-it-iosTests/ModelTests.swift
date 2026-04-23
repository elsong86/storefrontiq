import XCTest
@testable import taco_about_it_ios

final class ModelTests: XCTestCase {
    
    func testDisplayNameCodable() throws {
        // Given
        let original = DisplayName(text: "Test Place")
        
        // When
        let encoded = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(DisplayName.self, from: encoded)
        
        // Then
        XCTAssertEqual(original.text, decoded.text)
    }
    
    func testPlaceCodable() throws {
        // Given
        let original = Place(
            id: "test-id",
            displayName: DisplayName(text: "Test Place"),
            formattedAddress: "123 Test St",
            rating: 4.5,
            userRatingCount: 100,
            photos: []
        )
        
        // When
        let encoded = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(Place.self, from: encoded)
        
        // Then
        XCTAssertEqual(original.id, decoded.id)
        XCTAssertEqual(original.displayNameText, decoded.displayNameText)
    }
    
    func testPlacesResponseCodable() throws {
        // Given
        let place = Place(
            id: "test-id",
            displayName: DisplayName(text: "Test Place"),
            formattedAddress: "123 Test St",
            rating: 4.5,
            userRatingCount: 100,
            photos: []
        )
        let original = PlacesResponse(places: [place])
        
        // When
        let encoded = try JSONEncoder().encode(original)
        print(String(data: encoded, encoding: .utf8) ?? "")  // For debugging
        let decoded = try JSONDecoder().decode(PlacesResponse.self, from: encoded)
        
        // Then
        XCTAssertEqual(original.places.count, decoded.places.count)
        XCTAssertEqual(original.places[0].id, decoded.places[0].id)
    }
}
