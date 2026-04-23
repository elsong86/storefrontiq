import SwiftUI
import Observation

@Observable @MainActor
class ContentViewModel: ObservableObject {
    var location: GeoLocation?
    var places: [Place] = []
    var errorMessage: String?
   
    private let locationManager = LocationManager()
    private let placesService: PlacesServiceProtocol
    
    init(useMockData: Bool = false) {
        self.placesService = useMockData ? MockPlacesService() : PlacesService.shared
        if useMockData {
            self.places = MockData.places
        }
    }
    
    func refreshPlaces() async throws -> [Place] {
        let (_, places) = try await requestLocationAndFetchPlaces(forceRefresh: true)
        return places
    }
   
    func requestLocationAndFetchPlaces(forceRefresh: Bool = false) async throws -> (GeoLocation, [Place]) {
        do {
            let location = try await locationManager.requestLocationAsync()
            let geoLocation = GeoLocation(
                latitude: location.latitude,
                longitude: location.longitude
            )
            self.location = geoLocation
            
            do {
                let fetchedPlaces = try await placesService.fetchPlaces(
                    location: geoLocation,
                    radius: 1000.0,
                    maxResults: 20,
                    textQuery: "tacos",
                    forceRefresh: forceRefresh
                )
                self.places = fetchedPlaces
                return (geoLocation, fetchedPlaces)
            } catch {
                // If we got location but failed to fetch places, still show location
                self.errorMessage = "Could not fetch places: \(error.localizedDescription)"
                return (geoLocation, [])
            }
        } catch let error as LocationError {
            // More specific error handling for location errors
            throw error
        } catch {
            throw LocationError.unknown(error)
        }
    }
   
    func handleSearch(address: String, forceRefresh: Bool = false) async throws -> (GeoLocation, [Place]) {
        let location = try await geocodeAddress(address)
        self.location = location
        let fetchedPlaces = try await placesService.fetchPlaces(
            location: location,
            radius: 1000.0,
            maxResults: 20,
            textQuery: "tacos",
            forceRefresh: forceRefresh
        )
        self.places = fetchedPlaces
        return (location, fetchedPlaces)
    }
   
    func resetLocation() {
        self.location = nil
    }
}

// MARK: - Geocoding Extension
extension ContentViewModel {
    struct GeocodingResponse: Codable {
        let latitude: Double
        let longitude: Double
    }
   
    func geocodeAddress(_ address: String) async throws -> GeoLocation {
        let trimmedAddress = address.trimmingCharacters(in: .whitespaces)
        
        let url = URL(string: "\(PlacesService.shared.baseURL)/geocode")!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        // Get and use session token - with better error handling
        do {
            let token = try await SessionManager.shared.ensureValidSession()
            print("Got session token: \(token.prefix(10))...") // Print part of token for debugging
            request.setValue(token, forHTTPHeaderField: "X-Session-Token")
        } catch {
            print("Failed to get session token: \(error.localizedDescription)")
            throw error
        }
        
        let requestBody = ["address": trimmedAddress]
        let jsonData = try JSONEncoder().encode(requestBody)
        request.httpBody = jsonData
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            
            if let httpResponse = response as? HTTPURLResponse {
                print("HTTP response code: \(httpResponse.statusCode)")
                
                if !(200..<300).contains(httpResponse.statusCode) {
                    if let responseString = String(data: data, encoding: .utf8) {
                        print("Error response: \(responseString)")
                    }
                    
                    if httpResponse.statusCode == 401 {
                        try SessionManager.shared.clearSession()
                        // Retry once with a new session
                        return try await geocodeAddress(address)
                    }
                    
                    throw NSError(domain: "HTTP Error",
                                 code: httpResponse.statusCode,
                                 userInfo: [NSLocalizedDescriptionKey: String(data: data, encoding: .utf8) ?? "Unknown error"])
                }
            }
            
            let geocodeResponse = try JSONDecoder().decode(GeocodingResponse.self, from: data)
            return GeoLocation(latitude: geocodeResponse.latitude, longitude: geocodeResponse.longitude)
        } catch {
            print("Network request failed: \(error.localizedDescription)")
            
            // If it's an NSError, print more details
            if let nsError = error as NSError? {
                print("Error domain: \(nsError.domain), code: \(nsError.code)")
                print("Error user info: \(nsError.userInfo)")
            }
            
            throw error
        }
    }
}
