import Foundation

actor PlacesService: PlacesServiceProtocol {
    static let shared = PlacesService()
    let baseURL = "https://api.tacoaboutit.app"
    private let urlSession: URLSession
    
    // Photo request throttling
    private let photoRequestQueue = DispatchQueue(label: "com.tacoaboutit.photoRequests")
    private let photoRequestSemaphore = DispatchSemaphore(value: 4) // Max 4 concurrent requests
    
    // Cache for storing photo URLs to avoid repeated API calls
    private static let photoURLCache = NSCache<NSString, NSString>()
    
    // Debug flag - set to true to enable detailed URL cache logging
    private static let enableLogging = true
    
    init(urlSession: URLSession = .shared) {
        self.urlSession = urlSession
    }
    
    // Helper method to prepare authenticated requests
    private func prepareAuthenticatedRequest(_ request: inout URLRequest) async throws {
        // Get a valid session token
        let token = try await SessionManager.shared.ensureValidSession()
        
        // Add the token to the request
        request.addValue(token, forHTTPHeaderField: "X-Session-Token")
    }
    
    nonisolated func fetchPlaces(location: GeoLocation, radius: Double = 1000.0, maxResults: Int = 20, textQuery: String = "tacos", forceRefresh: Bool = false) async throws -> [Place] {
            // Generate cache key
            let cacheKey = await DiskCacheService.shared.placeSearchCacheKey(location: location, radius: radius, query: textQuery)
            
            // Try to get from cache first (unless force refresh requested)
            if !forceRefresh, let cachedPlaces: [Place] = await DiskCacheService.shared.retrieve(forKey: cacheKey) {
                print("Retrieved places from disk cache")
                // Optionally prefetch photos for cached places too if desired
                // Task { try? await self.prefetchPhotosForPlaces(Array(cachedPlaces.prefix(8))) }
                return cachedPlaces
            }
            
            print("Fetching places from network...") // Added log
            guard let url = URL(string: "\(baseURL)/places") else {
                throw NSError(domain: "Invalid URL", code: 0, userInfo: nil)
            }

            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.addValue("application/json", forHTTPHeaderField: "Content-Type")
            try await prepareAuthenticatedRequest(&request) // Adds X-Session-Token

            let body = PlacesRequest( // Assuming PlacesRequest struct exists and is Codable
                location: location,
                radius: radius,
                maxResults: maxResults,
                textQuery: textQuery
            )
            
            let encodedBody = try JSONEncoder().encode(body)
            request.httpBody = encodedBody

            // --- Network Call ---
            let (data, response) = try await urlSession.data(for: request)
            
            // --- Response Validation ---
            guard let httpResponse = response as? HTTPURLResponse else {
                throw NSError(domain: "Invalid response type", code: 0, userInfo: nil)
            }
             print("Fetch places HTTP status: \(httpResponse.statusCode)") // Added log

            // --- Log Raw Response ---
            if let jsonString = String(data: data, encoding: .utf8) {
                print("--- RAW JSON Response from /places ---")
                print(jsonString)
                print("--------------------------------------")
            } else {
                print("--- Failed to convert /places response data to UTF8 string ---")
            }

            // --- Handle Non-Success Status Codes ---
            guard (200..<300).contains(httpResponse.statusCode) else {
                // Handle session errors (like 401 Unauthorized)
                if httpResponse.statusCode == 401 {
                    print("Received 401 Unauthorized for /places. Clearing session and retrying...")
                    try await handleAPIError(httpResponse: httpResponse)
                    // Retry the request once (important: use forceRefresh true to bypass cache on retry)
                    return try await fetchPlaces(location: location, radius: radius, maxResults: maxResults, textQuery: textQuery, forceRefresh: true)
                }
                
                // Handle other non-200 errors
                var errorDetail = "HTTP error \(httpResponse.statusCode)"
                 if let responseString = String(data: data, encoding: .utf8) {
                      errorDetail += " - \(responseString)"
                 }
                print("Fetch places failed. \(errorDetail)")
                throw NSError(domain: "Invalid HTTP response", code: httpResponse.statusCode, userInfo: [NSLocalizedDescriptionKey: errorDetail])
            }
            
            // --- Attempt Decoding ---
            do {
                // Assuming PlacesResponse struct exists and contains `places: [Place]`
                let placesResponse = try JSONDecoder().decode(PlacesResponse.self, from: data)
                 print("Successfully decoded /places response.")

                // --- Cache the result AFTER successful fetch and decode ---
                Task.detached { // Use detached task if caching doesn't need actor context
                     await DiskCacheService.shared.cache(placesResponse.places, forKey: cacheKey, withExpiration: 3600) // 1 hour
                     print("Cached places search results.")
                }
                
                // --- Pre-fetch photo URLs AFTER successful fetch and decode ---
                Task.detached { // Use detached task if prefetching doesn't need actor context
                     try? await self.prefetchPhotosForPlaces(Array(placesResponse.places.prefix(8)))
                     print("Initiated photo prefetching for \(min(placesResponse.places.count, 8)) places.")
                }
                
                // --- Return the successfully decoded places ---
                return placesResponse.places

            } catch let decodeError {
                print("--- DECODING FAILED for /places response ---")
                print("Error: \(decodeError)")
                if let decodingError = decodeError as? DecodingError {
                     print("Decoding Error Context: \(decodingError)")
                }
                print("------------------------------------------")
                throw decodeError // Re-throw the decoding error
            }
            // No code should be after the do-catch block here
        }

    nonisolated func fetchReviews(for place: Place, forceRefresh: Bool = false) async throws -> ReviewAnalysisResponse {
        // Generate cache key
        let cacheKey = await DiskCacheService.shared.reviewsCacheKey(placeId: place.id)
        
        // Try to get from cache first (unless force refresh requested)
        if !forceRefresh, let cachedReviews: ReviewAnalysisResponse = await DiskCacheService.shared.retrieve(forKey: cacheKey) {
            print("Retrieved reviews from disk cache")
            return cachedReviews
        }
        
        guard let url = URL(string: "\(baseURL)/reviews") else {
            throw NSError(domain: "Invalid URL", code: 0, userInfo: nil)
        }
        
        var components = URLComponents(url: url, resolvingAgainstBaseURL: true)!
        components.queryItems = [
            URLQueryItem(name: "place_id", value: place.id),
            URLQueryItem(name: "displayName", value: place.displayName.text),
            URLQueryItem(name: "formattedAddress", value: place.formattedAddress ?? "")
        ]
        
        guard let finalUrl = components.url else {
            throw NSError(domain: "Invalid URL Components", code: 0, userInfo: nil)
        }
        
        var request = URLRequest(url: finalUrl)
        request.addValue("application/json", forHTTPHeaderField: "Content-Type")
        try await prepareAuthenticatedRequest(&request)

        
        let (data, response) = try await urlSession.data(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse else {
            throw NSError(domain: "Invalid response", code: 0, userInfo: nil)
        }
        
        guard (200..<300).contains(httpResponse.statusCode) else {
            // Handle session errors
            if httpResponse.statusCode == 401 {
                try await handleAPIError(httpResponse: httpResponse)
                // Retry the request once (optional)
                return try await fetchReviews(for: place, forceRefresh: true)
            }
            
            throw NSError(domain: "Invalid response", code: httpResponse.statusCode, userInfo: nil)
        }
        
        let reviewResponse = try JSONDecoder().decode(ReviewAnalysisResponse.self, from: data)
        
        // Cache the result after successful fetch
        Task {
            await DiskCacheService.shared.cache(reviewResponse, forKey: cacheKey, withExpiration: 24 * 3600) // 24 hours
        }
        
        return reviewResponse
    }
    
    private func prefetchPhotosForPlaces(_ places: [Place]) async throws {
        // Extract all unique photos to prefetch
        let photos = places.compactMap { $0.primaryPhoto }
        if !photos.isEmpty {
            _ = try await fetchPhotoURLsBatch(photos: photos, maxWidth: 160, maxHeight: 160)
        }
    }
    
    private func handleAPIError(httpResponse: HTTPURLResponse) async throws {
        // If we get a 401 Unauthorized, try creating a new session
        if httpResponse.statusCode == 401 {
            // Clear existing session
            try SessionManager.shared.clearSession()
        }
    }
    
    // MARK: - Photo URL Methods
    
    // Individual photo URL fetch - use when only one URL is needed
    nonisolated func fetchPhotoURL(for photo: Photo, maxWidth: Int = 400, maxHeight: Int? = nil) async throws -> URL {
        // Create a cache key from the photo name and dimensions
        let dimensionString = maxHeight != nil ? "\(maxWidth)x\(maxHeight!)" : "\(maxWidth)"
        let cacheKeyString = "\(photo.name)_\(dimensionString)"
        
        // Check if we already have this URL cached
        if let cachedURLString = Self.photoURLCache.object(forKey: NSString(string: cacheKeyString)) {
            if Self.enableLogging {
                print("🔵 URL CACHE HIT: \(shortenPhotoName(photo.name)) -> \(dimensionString)")
            }
            return URL(string: cachedURLString as String)!
        }
        
        if Self.enableLogging {
            print("⚪ URL cache miss: \(shortenPhotoName(photo.name)) -> \(dimensionString)")
        }
        
        // If not cached, fetch from the API
        return try await performPhotoURLRequest(
            photo: photo,
            maxWidth: maxWidth,
            maxHeight: maxHeight,
            cacheKeyString: cacheKeyString
        )
    }
    
    // Batch photo URL fetch - use when loading multiple photos at once
    nonisolated func fetchPhotoURLsBatch(photos: [Photo], maxWidth: Int = 400, maxHeight: Int? = nil) async throws -> [String: URL] {
        // First check cache for each photo
        var result: [String: URL] = [:]
        var uncachedPhotos: [Photo] = []
        var cacheKeys: [String: String] = [:]
        
        // Check cache first
        for photo in photos {
            let dimensionString = maxHeight != nil ? "\(maxWidth)x\(maxHeight!)" : "\(maxWidth)"
            let cacheKeyString = "\(photo.name)_\(dimensionString)"
            
            if let cachedURLString = Self.photoURLCache.object(forKey: NSString(string: cacheKeyString)),
               let url = URL(string: cachedURLString as String) {
                result[photo.name] = url
                
                if Self.enableLogging {
                    print("🔵 URL CACHE HIT (batch): \(shortenPhotoName(photo.name))")
                }
            } else {
                uncachedPhotos.append(photo)
                cacheKeys[photo.name] = cacheKeyString
            }
        }
        
        // If all photos were cached, return immediately
        if uncachedPhotos.isEmpty {
            return result
        }
        
        // Process uncached photos in batches to avoid overwhelming the server
        let batchSize = 5
        for batch in stride(from: 0, to: uncachedPhotos.count, by: batchSize) {
            let end = min(batch + batchSize, uncachedPhotos.count)
            let currentBatch = Array(uncachedPhotos[batch..<end])
            
            // Create tasks for each photo in the batch
            await withTaskGroup(of: (String, URL?).self) { group in
                for photo in currentBatch {
                    group.addTask {
                        do {
                            let url = try await self.performPhotoURLRequest(
                                photo: photo,
                                maxWidth: maxWidth,
                                maxHeight: maxHeight,
                                cacheKeyString: cacheKeys[photo.name] ?? ""
                            )
                            return (photo.name, url)
                        } catch {
                            print("⚠️ Error fetching photo URL for \(self.shortenPhotoName(photo.name)): \(error.localizedDescription)")
                            return (photo.name, nil)
                        }
                    }
                }
                
                // Process results as they complete
                for await (name, url) in group {
                    if let url = url {
                        result[name] = url
                    }
                }
            }
            
            // Small delay between batches to be nice to the API
            if end < uncachedPhotos.count {
                try await Task.sleep(nanoseconds: 100_000_000) // 100ms
            }
        }
        
        return result
    }
    
    // Helper method to make the actual API request for a photo URL
    private nonisolated func performPhotoURLRequest(
        photo: Photo,
        maxWidth: Int,
        maxHeight: Int?,
        cacheKeyString: String
    ) async throws -> URL {
        // Double-check cache before making the request
        if let cachedURLString = Self.photoURLCache.object(forKey: NSString(string: cacheKeyString)),
           let url = URL(string: cachedURLString as String) {
            if Self.enableLogging {
                print("🔵 URL CACHE HIT (double-check): \(shortenPhotoName(photo.name))")
            }
            return url
        }
        
        guard let url = URL(string: "\(baseURL)/photos") else {
            throw NSError(domain: "Invalid URL", code: 0, userInfo: nil)
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.addValue("application/json", forHTTPHeaderField: "Content-Type")
        try await prepareAuthenticatedRequest(&request)

        let photoRequest = PhotoRequest(
            photoName: photo.name,
            maxHeight: maxHeight,
            maxWidth: maxWidth
        )
        
        let encodedBody = try JSONEncoder().encode(photoRequest)
        request.httpBody = encodedBody
        
        if Self.enableLogging {
            print("📡 FETCHING URL from API: \(shortenPhotoName(photo.name))")
        }
        
        let (data, response) = try await urlSession.data(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse else {
            throw NSError(domain: "Invalid response", code: 0, userInfo: nil)
        }
        
        guard (200..<300).contains(httpResponse.statusCode) else {
            // Handle session errors
            if httpResponse.statusCode == 401 {
                try await handleAPIError(httpResponse: httpResponse)
                // Retry the request once
                return try await performPhotoURLRequest(
                    photo: photo,
                    maxWidth: maxWidth,
                    maxHeight: maxHeight,
                    cacheKeyString: cacheKeyString
                )
            }
            
            throw NSError(
                domain: "Invalid response",
                code: httpResponse.statusCode,
                userInfo: [NSLocalizedDescriptionKey: "HTTP error \(httpResponse.statusCode)"]
            )
        }
        
        let photoResponse = try JSONDecoder().decode(PhotoResponse.self, from: data)
        guard let photoURL = URL(string: photoResponse.url) else {
            throw NSError(domain: "Invalid photo URL", code: 0, userInfo: nil)
        }
        
        // Cache the result
        Self.photoURLCache.setObject(
            NSString(string: photoResponse.url),
            forKey: NSString(string: cacheKeyString)
        )
        
        if Self.enableLogging {
            print("🔵 Cached URL: \(shortenPhotoName(photo.name)) → \(shortenURL(photoResponse.url))")
        }
        
        return photoURL
    }
    
    // MARK: - Helper Methods
    
    // Helper to shorten photo name for logging
    private nonisolated func shortenPhotoName(_ photoName: String) -> String {
        let components = photoName.components(separatedBy: "/")
        if components.count > 1 {
            return "..." + components.suffix(2).joined(separator: "/")
        }
        return photoName
    }
    
    // Helper to shorten URLs for logging
    private nonisolated func shortenURL(_ urlString: String) -> String {
        if urlString.count < 40 {
            return urlString
        }
        
        guard let url = URL(string: urlString) else { return urlString }
        let host = url.host ?? ""
        let query = url.query?.prefix(30) ?? ""
        return "\(host)/...\(query)..."
    }
}
