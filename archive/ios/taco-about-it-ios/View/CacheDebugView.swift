import SwiftUI

/// A debug view to test and monitor caching performance
/// This can be integrated as a developer tool in your app
struct CacheDebugView: View {
    @State private var refreshNumber = 0
    @State private var loadTimes: [Double] = []
    @State private var photoURLs: [URL] = []
    @State private var isClearing = false
    
    // Sample photo for testing
    let samplePhoto = Place.mockPlace.primaryPhoto ?? Photo(name: "places/ChIJTWE_0BtxK4gRVCQ")
    
    // Track cache hits and misses
    @State private var memCacheHits = 0
    @State private var diskCacheHits = 0
    @State private var networkLoads = 0
    
    var body: some View {
        NavigationView {
            List {
                Section(header: Text("Cache Statistics")) {
                    statsRow(title: "Memory Cache Hits", value: memCacheHits)
                    statsRow(title: "Disk Cache Hits", value: diskCacheHits)
                    statsRow(title: "Network Loads", value: networkLoads)
                    
                    if !loadTimes.isEmpty {
                        statsRow(title: "Average Load Time", value: "\(Int(loadTimes.reduce(0, +) / Double(loadTimes.count)))ms")
                        statsRow(title: "Last Load Time", value: "\(Int(loadTimes.last ?? 0))ms")
                    }
                }
                
                Section(header: Text("Actions")) {
                    Button("Load Image (Same Size)") {
                        loadImageWithSize(width: 400, height: 400)
                    }
                    
                    Button("Load Image (New Size)") {
                        let randomDimension = Int.random(in: 200...600)
                        loadImageWithSize(width: randomDimension, height: randomDimension)
                    }
                    
                    Button("Clear Memory Cache") {
                        withAnimation {
                            isClearing = true
                        }
                        // Use the new nonisolated wrapper method
                        ImageCacheService.shared.clearCacheAsync()
                        
                        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                            withAnimation {
                                isClearing = false
                            }
                        }
                    }
                    
                    Button("Simulate Memory Warning") {
                        NotificationCenter.default.post(name: UIApplication.didReceiveMemoryWarningNotification, object: nil)
                    }
                }
                
                Section(header: Text("Test Images")) {
                    ForEach(photoURLs, id: \.self) { url in
                        VStack(alignment: .leading) {
                            Text(url.lastPathComponent)
                                .font(.caption)
                                .foregroundColor(.secondary)
                                .lineLimit(1)
                            
                            HStack {
                                // Refresh the image by adding a query parameter
                                // This tests if the cache is working
                                ImageCacheService.shared.cachedImage(
                                    url: url.appendingQueryParameter(name: "refresh", value: String(refreshNumber))
                                )
                                .aspectRatio(contentMode: .fill)
                                .frame(width: 120, height: 80)
                                .cornerRadius(8)
                                .clipped()
                                
                                VStack(alignment: .leading) {
                                    Text("Width: \(url.queryParameters["maxWidthPx"] ?? "unknown")")
                                        .font(.caption)
                                    Text("Height: \(url.queryParameters["maxHeightPx"] ?? "unknown")")
                                        .font(.caption)
                                }
                            }
                        }
                        .padding(.vertical, 4)
                    }
                }
            }
            .navigationTitle("Cache Debug")
            .overlay(
                Group {
                    if isClearing {
                        ProgressView("Clearing cache...")
                            .padding()
                            .background(
                                RoundedRectangle(cornerRadius: 8)
                                    .fill(Color(.systemBackground))
                                    .shadow(radius: 2)
                            )
                    }
                }
            )
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Refresh") {
                        refreshNumber += 1
                    }
                }
            }
        }
        // Monitor console logs to track cache hits and misses
        .onAppear {
            setupCacheMonitoring()
        }
    }
    
    private func loadImageWithSize(width: Int, height: Int) {
        Task {
            do {
                let startTime = CFAbsoluteTimeGetCurrent()
                let url = try await PlacesService.shared.fetchPhotoURL(
                    for: samplePhoto,
                    maxWidth: width,
                    maxHeight: height
                )
                let timeElapsed = (CFAbsoluteTimeGetCurrent() - startTime) * 1000
                
                await MainActor.run {
                    photoURLs.append(url)
                    loadTimes.append(timeElapsed)
                }
            } catch {
                print("Error loading photo: \(error.localizedDescription)")
            }
        }
    }
    
    private func setupCacheMonitoring() {
        // In a real implementation, you could hook into the cache service
        // to monitor actual cache hits and misses
        
        // For now, this is just a placeholder
        // You'll need to look at the console logs to see the actual cache behavior
        print("🔍 Cache monitoring started - watch console logs for details")
    }
    
    private func statsRow(title: String, value: Int) -> some View {
        HStack {
            Text(title)
            Spacer()
            Text("\(value)")
                .foregroundColor(.secondary)
        }
    }
    
    private func statsRow(title: String, value: String) -> some View {
        HStack {
            Text(title)
            Spacer()
            Text(value)
                .foregroundColor(.secondary)
        }
    }
}

// Helper extension to add query parameters to URLs
extension URL {
    func appendingQueryParameter(name: String, value: String) -> URL {
        var components = URLComponents(url: self, resolvingAgainstBaseURL: true)!
        var queryItems = components.queryItems ?? []
        queryItems.append(URLQueryItem(name: name, value: value))
        components.queryItems = queryItems
        return components.url!
    }
    
    var queryParameters: [String: String] {
        var params: [String: String] = [:]
        guard let components = URLComponents(url: self, resolvingAgainstBaseURL: true),
              let queryItems = components.queryItems else { return params }
        
        for item in queryItems {
            params[item.name] = item.value
        }
        return params
    }
}

#Preview {
    CacheDebugView()
}
