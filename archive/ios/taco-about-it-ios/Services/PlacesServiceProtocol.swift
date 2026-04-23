import Foundation

protocol PlacesServiceProtocol {
    func fetchPlaces(location: GeoLocation, radius: Double, maxResults: Int, textQuery: String, forceRefresh: Bool) async throws -> [Place]
    func fetchReviews(for place: Place, forceRefresh: Bool) async throws -> ReviewAnalysisResponse
}
extension PlacesServiceProtocol {
    func fetchPhotoURL(for photo: Photo, maxWidth: Int = 400, maxHeight: Int? = nil) async throws -> URL {
        // Default implementation for the protocol
        // This will be overridden by the actual implementation in PlacesService
        throw NSError(domain: "Not implemented", code: 0, userInfo: nil)
    }
}
