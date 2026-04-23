// MockData.swift
import Foundation

enum MockData {
    static let location = GeoLocation(
        latitude: 37.7749,
        longitude: -122.4194
    )
    
    // Sample taco restaurant names for variety
    private static let restaurantNames = [
        "El Taco Loco", "Taqueria La Mejor", "Tacos El Rey",
        "La Taqueria", "El Farolito", "Taco Bell",
        "Super Tacos", "Los Coyotes", "Taqueria San Jose",
        "King Taco", "Guisados", "Mi Ranchito Taco Shop",
        "Taqueria Guadalajara", "El Gallo Giro", "Titos Tacos",
        "Pinches Tacos", "Taco Temple", "Tacos Morenos",
        "Tacos Por Favor", "Mexicali Taco & Co"
    ]
    
    // Mock photo names that simulate Google Places photo resource names
    private static let mockPhotoNames = [
        "places/ChIJa8K5XnmmI4gRTUwGh6e7AZU/photos/AWU5eFg0b15q",
        "places/ChIJN1t_tDeuEmsRUsoyG83frY4/photos/BXYZ1234k15q",
        "places/ChIJ7cv00DwsDogRAMDACha2mQ0/photos/CWUabCd0q24r",
        "places/ChIJO3qMc8gDdkgRu8siOXHqt7w/photos/DZYDefG1r35s",
        "places/ChIJ2eUgeAK6j4ARbn5u_wAGqWA/photos/EFGhIjK2s46t"
    ]
    
    // Generate a sample photo for each place
    private static func generateMockPhoto(for index: Int) -> Photo {
        let photoIndex = index % mockPhotoNames.count
        return Photo(name: mockPhotoNames[photoIndex])
    }
    
    // Generate a large number of mock places for testing
    static let places: [Place] = (0..<200).map { index in
        let nameIndex = index % restaurantNames.count
        let streetNumber = 100 + index
        let streetNames = ["Mission St", "Valencia St", "Market St", "Castro St", "Hayes St"]
        let streetIndex = index % streetNames.count
        
        return Place(
            id: "mock-place-\(index)",
            displayName: DisplayName(text: "\(restaurantNames[nameIndex]) #\(index/restaurantNames.count + 1)"),
            formattedAddress: "\(streetNumber) \(streetNames[streetIndex]), San Francisco, CA 9411\(index % 10)",
            rating: Double.random(in: 3.0...5.0).rounded(to: 1),
            userRatingCount: Int.random(in: 50...500),
            photos: [generateMockPhoto(for: index)]
        )
    }
    
    // Keep a smaller set of the original sample reviews
    static let reviews: [Review] = [
        Review(reviewText: "Best tacos in the Mission! The al pastor is incredible and their salsa verde is perfectly spicy. The tortillas are always fresh and handmade."),
        Review(reviewText: "Great authentic Mexican food. You can really taste the difference with their homemade tortillas. The carnitas are tender and flavorful."),
        Review(reviewText: "Amazing spot for late night tacos. Their horchata is the perfect balance of sweet and creamy. Prices are reasonable for the quality."),
        Review(reviewText: "The carne asada is cooked to perfection. Service can be a bit slow during peak hours but the food is worth the wait."),
        Review(reviewText: "Their fish tacos are surprisingly good! The batter is light and crispy, and the chipotle crema adds the perfect kick.")
    ]
    
    // Generate review responses for all mock places
    static let reviewResponses: [String: ReviewAnalysisResponse] = Dictionary(
        uniqueKeysWithValues: places.map { place in
            (
                place.id,
                ReviewAnalysisResponse(
                    averageSentiment: Double.random(in: 7.0...9.5).rounded(to: 1),
                    reviews: Array(reviews.shuffled().prefix(Int.random(in: 2...5))),
                    source: "Mock Data"
                )
            )
        }
    )
}

// Helper extension to round doubles to specified decimal places
extension Double {
    func rounded(to places: Int) -> Double {
        let divisor = pow(10.0, Double(places))
        return (self * divisor).rounded() / divisor
    }
}


// MARK: - Preview Helpers
extension Place {
    static var mockPlace: Place {
        MockData.places[0]
    }
    
    static var mockPlaces: [Place] {
        MockData.places
    }
}

extension Review {
    static var mockReview: Review {
        MockData.reviews[0]
    }
    
    static var mockReviews: [Review] {
        MockData.reviews
    }
}

extension ReviewAnalysisResponse {
    static var mockResponse: ReviewAnalysisResponse {
        MockData.reviewResponses[MockData.places[0].id]!
    }
}

// MARK: - Mock Services
class MockPlacesService: PlacesServiceProtocol {
    func fetchPlaces(location: GeoLocation, radius: Double = 1000.0, maxResults: Int = 20, textQuery: String = "tacos") async throws -> [Place] {
        return MockData.places
    }
    
    func fetchReviews(for place: Place) async throws -> ReviewAnalysisResponse {
        return MockData.reviewResponses[place.id] ?? ReviewAnalysisResponse(
            averageSentiment: 8.5,
            reviews: Array(MockData.reviews.prefix(3)),
            source: "Mock Data"
        )
    }
}

// MARK: - Preview Content Helpers
extension ContentViewModel {
    static var preview: ContentViewModel {
        let viewModel = ContentViewModel(useMockData: true)
        viewModel.location = MockData.location
        viewModel.places = MockData.places
        return viewModel
    }
}

extension PlacesViewModel {
    static var preview: PlacesViewModel {
        PlacesViewModel(prefetchedPlaces: MockData.places)
    }
}

extension MockPlacesService {
    func fetchPlaces(location: GeoLocation, radius: Double = 1000.0, maxResults: Int = 20, textQuery: String = "tacos", forceRefresh: Bool = false) async throws -> [Place] {
            return MockData.places
        }
        
        func fetchReviews(for place: Place, forceRefresh: Bool = false) async throws -> ReviewAnalysisResponse {
            return MockData.reviewResponses[place.id] ?? ReviewAnalysisResponse(
                averageSentiment: 8.5,
                reviews: Array(MockData.reviews.prefix(3)),
                source: "Mock Data"
            )
        }
}
