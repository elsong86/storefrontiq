import SwiftUI
import Foundation

// A service to handle image caching
actor ImageCacheService {
    static let shared = ImageCacheService()
    
    // NSCache for in-memory caching of images
    private var imageCache = NSCache<NSString, UIImage>()
    
    // Debug flag - set to true to enable detailed cache logging
    private let enableLogging = true
    
    init() {
        // Configure cache limits
        imageCache.countLimit = 100 // Max number of images to keep in memory
        
        // Configure URLCache for disk caching if not already done
        let cacheSizeMemory = 50 * 1024 * 1024 // 50 MB
        let cacheSizeDisk = 100 * 1024 * 1024 // 100 MB
        let urlCache = URLCache(memoryCapacity: cacheSizeMemory, diskCapacity: cacheSizeDisk)
        URLCache.shared = urlCache
        
        if enableLogging {
            // This is safe to call directly since it doesn't use any actor state
            Self.logCacheStatus()
        }
    }
    
    // Make this static and nonisolated since it doesn't use actor state
    private static func logCacheStatus() {
        let urlCache = URLCache.shared
        let memoryUsageMB = Double(urlCache.currentMemoryUsage) / (1024 * 1024)
        let diskUsageMB = Double(urlCache.currentDiskUsage) / (1024 * 1024)
        
        print("📦 CACHE STATUS 📦")
        print("Memory cache capacity: \(Double(urlCache.memoryCapacity) / (1024 * 1024)) MB")
        print("Current memory usage: \(memoryUsageMB) MB")
        print("Disk cache capacity: \(Double(urlCache.diskCapacity) / (1024 * 1024)) MB")
        print("Current disk usage: \(diskUsageMB) MB")
    }
    
    // Clear the cache (useful for debugging or low memory situations)
    func clearCache() {
        if enableLogging {
            print("🗑️ CLEARING CACHE 🗑️")
        }
        imageCache.removeAllObjects()
        URLCache.shared.removeAllCachedResponses()
        
        if enableLogging {
            Self.logCacheStatus()
        }
    }
    
    nonisolated func clearCacheAsync() {
        Task {
            await clearCache()
        }
    }
    
    // Check if an image is in memory cache
    func isImageCached(for urlString: String) -> Bool {
        let cacheKey = urlString as NSString
        return imageCache.object(forKey: cacheKey) != nil
    }
    
    // Store an image in memory cache
    func storeImage(_ image: UIImage, for urlString: String) {
        let cacheKey = urlString as NSString
        imageCache.setObject(image, forKey: cacheKey)
        
        if enableLogging {
            print("🏎️ Saved to memory cache: \(shortenURL(urlString))")
        }
    }
    
    // Get an image from memory cache
    func getCachedImage(for urlString: String) -> UIImage? {
        let cacheKey = urlString as NSString
        let image = imageCache.object(forKey: cacheKey)
        
        if image != nil && enableLogging {
            print("🏎️ MEMORY CACHE HIT: \(shortenURL(urlString))")
        }
        
        return image
    }
    
    // Load an image from URL with caching
    func loadImage(url: URL) async throws -> UIImage {
        let urlString = url.absoluteString
        
        // Check in-memory cache first
        if let cachedImage = getCachedImage(for: urlString) {
            return cachedImage
        }
        
        if enableLogging {
            print("🔍 Memory cache miss: \(shortenURL(urlString))")
        }
        
        // Create a URLRequest that allows caching
        var request = URLRequest(url: url)
        request.cachePolicy = .returnCacheDataElseLoad
        
        // Check if we have the response in URLCache (disk cache)
        if let cachedResponse = URLCache.shared.cachedResponse(for: request) {
            if enableLogging {
                print("💾 DISK CACHE HIT: \(shortenURL(urlString))")
            }
            
            if let image = UIImage(data: cachedResponse.data) {
                // Store in memory cache too for faster future access
                storeImage(image, for: urlString)
                return image
            }
        }
        
        // If we got here, we need to download from network
        if enableLogging {
            print("📡 DOWNLOADING: \(shortenURL(urlString))")
        }
        
        // Fetch the image data from network
        let (data, response) = try await URLSession.shared.data(for: request)
        
        // Convert data to UIImage
        guard let image = UIImage(data: data), let httpResponse = response as? HTTPURLResponse else {
            throw NSError(domain: "ImageCacheService", code: 100, userInfo: [NSLocalizedDescriptionKey: "Failed to create image from data"])
        }
        
        // Store response in URLCache (disk cache)
        if httpResponse.statusCode == 200 {
            let cachedResponse = CachedURLResponse(response: response, data: data)
            URLCache.shared.storeCachedResponse(cachedResponse, for: request)
            
            if enableLogging {
                print("💾 Saved to disk cache: \(shortenURL(urlString))")
                Self.logCacheStatus()
            }
        }
        
        // Store in memory cache
        storeImage(image, for: urlString)
        
        return image
    }
    
    // Prefetch images for places
    func prefetchImagesForPlaces(_ places: [Place]) async {
        // Get photos to prefetch
        let photosToFetch = places.compactMap { $0.primaryPhoto }
        
        if !photosToFetch.isEmpty {
            do {
                // Use a standard size for thumbnails
                let maxWidth = 160
                let maxHeight = 160
                
                let urls = try await PlacesService.shared.fetchPhotoURLsBatch(
                    photos: photosToFetch,
                    maxWidth: maxWidth,
                    maxHeight: maxHeight
                )
                
                // Prefetch the images themselves in parallel
                await withTaskGroup(of: Void.self) { group in
                    for (_, url) in urls {
                        group.addTask {
                            do {
                                _ = try await self.loadImage(url: url)
                            } catch {
                                // Silently fail on individual image loads
                                print("Failed to prefetch image: \(error.localizedDescription)")
                            }
                        }
                    }
                }
            } catch {
                print("Prefetch error: \(error)")
            }
        }
    }
    
    // Helper to shorten URLs for logging - make this static and nonisolated
    private static func shortenURL(_ urlString: String) -> String {
        // Get last path component or a portion of the URL to display
        guard let url = URL(string: urlString) else { return urlString }
        if urlString.count < 40 {
            return urlString
        }
        
        let lastPathComponent = url.lastPathComponent
        let host = url.host ?? ""
        return "\(host)/...\(lastPathComponent.prefix(20))...\(lastPathComponent.count - 20) chars"
    }
    
    // Instance method version that calls the static version
    private func shortenURL(_ urlString: String) -> String {
        return Self.shortenURL(urlString)
    }
}

// A SwiftUI view that uses our caching system
struct CachedAsyncImage: View {
    let url: URL
    let placeholder: Image
    
    @State private var image: UIImage?
    @State private var isLoading = false
    @State private var loadError = false
    
    var body: some View {
        Group {
            if let image = image {
                Image(uiImage: image)
                    .resizable()
                    .aspectRatio(contentMode: .fill)
            } else if isLoading {
                ProgressView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .background(Color.gray.opacity(0.1))
            } else if loadError {
                placeholder
                    .foregroundColor(.gray)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .background(Color.gray.opacity(0.1))
            } else {
                placeholder
                    .foregroundColor(.gray)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .background(Color.gray.opacity(0.1))
                    .onAppear {
                        loadImage()
                    }
            }
        }
    }
    
    private func loadImage() {
        isLoading = true
        loadError = false
        
        Task {
            do {
                let loadedImage = try await ImageCacheService.shared.loadImage(url: url)
                await MainActor.run {
                    self.image = loadedImage
                    self.isLoading = false
                }
            } catch {
                await MainActor.run {
                    self.loadError = true
                    self.isLoading = false
                }
            }
        }
    }
}

// Extension to create a view helper
extension ImageCacheService {
    nonisolated func cachedImage(url: URL, placeholder: Image = Image(systemName: "fork.knife")) -> some View {
        CachedAsyncImage(url: url, placeholder: placeholder)
    }
}
