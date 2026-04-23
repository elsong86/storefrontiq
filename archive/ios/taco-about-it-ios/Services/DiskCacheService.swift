import Foundation

actor DiskCacheService {
    static let shared = DiskCacheService()
    
    private let fileManager = FileManager.default
    private let cacheDirectory: URL
    
    // Cache durations
    private let placeCacheDuration: TimeInterval = 48 * 3600 // 48 hours
    private let reviewCacheDuration: TimeInterval = 24 * 3600 // 24 hours
    private let searchCacheDuration: TimeInterval = 6 * 3600  // 6 hours
    
    private init() {
        // Get the app's cache directory
        let cacheDirectoryURL = fileManager.urls(for: .cachesDirectory, in: .userDomainMask).first!
        self.cacheDirectory = cacheDirectoryURL.appendingPathComponent("AppDataCache", isDirectory: true)
        
        // Create cache directory if it doesn't exist
        try? fileManager.createDirectory(at: cacheDirectory, withIntermediateDirectories: true)
        
        // Clean up expired cache on initialization
        Task {
            await cleanupExpiredCache()
        }
    }
    
    // Generic function to save data to disk
    func cache<T: Encodable>(_ object: T, forKey key: String, withExpiration duration: TimeInterval? = nil) {
        do {
            let data = try JSONEncoder().encode(object)
            let metadata = CacheMetadata(timestamp: Date(), expirationDuration: duration)
            let metadataData = try JSONEncoder().encode(metadata)
            
            let filePath = cacheDirectory.appendingPathComponent("\(key).cache")
            let metadataPath = cacheDirectory.appendingPathComponent("\(key).metadata")
            
            try data.write(to: filePath)
            try metadataData.write(to: metadataPath)
        } catch {
            print("Error caching object: \(error)")
        }
    }
    
    // Generic function to retrieve data from disk
    func retrieve<T: Decodable>(forKey key: String) -> T? {
        let filePath = cacheDirectory.appendingPathComponent("\(key).cache")
        let metadataPath = cacheDirectory.appendingPathComponent("\(key).metadata")
        
        guard fileManager.fileExists(atPath: filePath.path),
              fileManager.fileExists(atPath: metadataPath.path) else {
            return nil
        }
        
        do {
            // Check if cache is expired
            let metadataData = try Data(contentsOf: metadataPath)
            let metadata = try JSONDecoder().decode(CacheMetadata.self, from: metadataData)
            
            if let expirationDuration = metadata.expirationDuration,
               Date().timeIntervalSince(metadata.timestamp) > expirationDuration {
                // Cache is expired, delete files
                try? fileManager.removeItem(at: filePath)
                try? fileManager.removeItem(at: metadataPath)
                return nil
            }
            
            // Cache is valid, retrieve data
            let data = try Data(contentsOf: filePath)
            return try JSONDecoder().decode(T.self, from: data)
        } catch {
            print("Error retrieving cached object: \(error)")
            return nil
        }
    }
    
    // Mark a specific cache item for refresh on next fetch
    func markForRefresh(key: String) {
        let metadataPath = cacheDirectory.appendingPathComponent("\(key).metadata")
        
        if fileManager.fileExists(atPath: metadataPath.path) {
            try? fileManager.removeItem(at: metadataPath)
        }
    }
    
    // Internal method to clean up expired cache
    private func cleanupExpiredCache() async {
        do {
            let fileURLs = try fileManager.contentsOfDirectory(at: cacheDirectory, includingPropertiesForKeys: nil)
            let metadataURLs = fileURLs.filter { $0.pathExtension == "metadata" }
            
            var totalRemoved = 0
            
            for metadataURL in metadataURLs {
                do {
                    let metadataData = try Data(contentsOf: metadataURL)
                    let metadata = try JSONDecoder().decode(CacheMetadata.self, from: metadataData)
                    
                    if let expirationDuration = metadata.expirationDuration,
                       Date().timeIntervalSince(metadata.timestamp) > expirationDuration {
                        let key = metadataURL.deletingPathExtension().lastPathComponent
                        let filePath = cacheDirectory.appendingPathComponent("\(key).cache")
                        
                        try? fileManager.removeItem(at: filePath)
                        try? fileManager.removeItem(at: metadataURL)
                        totalRemoved += 1
                    }
                } catch {
                    // Skip this metadata file if there's an issue
                    continue
                }
            }
            
            // Also check for orphaned cache files (without metadata)
            let cacheURLs = fileURLs.filter { $0.pathExtension == "cache" }
            for cacheURL in cacheURLs {
                let baseName = cacheURL.deletingPathExtension().lastPathComponent
                let metadataPath = cacheDirectory.appendingPathComponent("\(baseName).metadata")
                
                if !fileManager.fileExists(atPath: metadataPath.path) {
                    try? fileManager.removeItem(at: cacheURL)
                    totalRemoved += 1
                }
            }
            
            if totalRemoved > 0 {
                print("Disk cache cleanup: removed \(totalRemoved) expired items")
            }
            
            // Perform cache size check and cleanup if needed
            try manageCacheSize()
            
        } catch {
            print("Error cleaning expired cache: \(error)")
        }
    }
    
    // Monitor and manage overall cache size
    private func manageCacheSize() throws {
        // Target maximum cache size (100MB)
        let maxCacheSize: UInt64 = 100 * 1024 * 1024
        
        // Get all files in cache directory
        let fileURLs = try fileManager.contentsOfDirectory(at: cacheDirectory, includingPropertiesForKeys: nil)
        
        // Calculate current size and get file attributes
        var currentSize: UInt64 = 0
        var fileAttributes: [(url: URL, size: UInt64, date: Date)] = []
        
        for fileURL in fileURLs {
            let attributes = try fileManager.attributesOfItem(atPath: fileURL.path)
            if let fileSize = attributes[.size] as? UInt64,
               let modificationDate = attributes[.modificationDate] as? Date {
                currentSize += fileSize
                fileAttributes.append((fileURL, fileSize, modificationDate))
            }
        }
        
        // If we're over size limit, remove oldest files until under target
        if currentSize > maxCacheSize {
            // Sort by modification date (oldest first)
            fileAttributes.sort { $0.date < $1.date }
            
            var removedSize: UInt64 = 0
            let targetRemoval = currentSize - maxCacheSize
            
            // Remove oldest files until we're under the limit
            for fileAttr in fileAttributes {
                try fileManager.removeItem(at: fileAttr.url)
                removedSize += fileAttr.size
                
                if removedSize >= targetRemoval {
                    break
                }
            }
            
            print("Cache size management: removed \(removedSize) bytes")
        }
    }
    
    // Method to handle periodic cleanup
    func performMaintenance() {
        Task {
            await cleanupExpiredCache()
        }
    }
}

// Metadata to track cache age and expiration
struct CacheMetadata: Codable {
    let timestamp: Date
    let expirationDuration: TimeInterval?
}

extension DiskCacheService {
    // Generate cache key for places search
    func placeSearchCacheKey(location: GeoLocation, radius: Double, query: String) -> String {
        let roundedLat = (location.latitude * 100).rounded() / 100
        let roundedLng = (location.longitude * 100).rounded() / 100
        return "places_search_\(roundedLat)_\(roundedLng)_\(Int(radius))_\(query)"
    }
    
    // Generate cache key for place details
    func placeDetailsCacheKey(placeId: String) -> String {
        return "place_details_\(placeId)"
    }
    
    // Generate cache key for reviews
    func reviewsCacheKey(placeId: String) -> String {
        return "reviews_\(placeId)"
    }
}
