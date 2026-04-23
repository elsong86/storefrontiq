import SwiftUI

/// A reusable component for displaying a Place photo with caching
struct CachedPlacePhotoView: View {
    let photo: Photo?
    var width: Int = 400
    var height: Int? = nil
    var contentMode: ContentMode = .fill
    var cornerRadius: CGFloat = 0
    
    @State private var photoURL: URL? = nil
    @State private var isLoading = false
    
    var body: some View {
        Group {
            if let photoURL = photoURL {
                // This method is nonisolated and is safe to call directly
                ImageCacheService.shared.cachedImage(
                    url: photoURL,
                    placeholder: Image(systemName: "fork.knife")
                )
                .aspectRatio(contentMode: contentMode)
                .cornerRadius(cornerRadius)
            } else if isLoading {
                ProgressView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .background(Color.gray.opacity(0.1))
                    .cornerRadius(cornerRadius)
            } else {
                Image(systemName: "fork.knife")
                    .font(.system(size: 30))
                    .foregroundColor(.gray)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .background(Color.gray.opacity(0.1))
                    .cornerRadius(cornerRadius)
            }
        }
        .task {
            await loadPhoto()
        }
    }
    
    private func loadPhoto() async {
        // Skip if no photo or already loading
        guard let photo = photo, !isLoading, photoURL == nil else {
            return
        }
        
        isLoading = true
        defer { isLoading = false }
        
        do {
            // Add small delay between requests to avoid overwhelming API
            try await Task.sleep(nanoseconds: UInt64.random(in: 50_000_000...200_000_000))
            
            photoURL = try await PlacesService.shared.fetchPhotoURL(
                for: photo,
                maxWidth: width,
                maxHeight: height
            )
        } catch {
            print("Photo loading error: \(error.localizedDescription)")
        }
    }
}

// Extension for using the view with an optional photo
extension CachedPlacePhotoView {
    /// Create a photo view with an optional Photo
    /// - Returns: The view or a placeholder if photo is nil
    static func optional(
        _ photo: Photo?,
        width: Int = 400,
        height: Int? = nil,
        contentMode: ContentMode = .fill,
        cornerRadius: CGFloat = 0
    ) -> some View {
        if let photo = photo {
            return CachedPlacePhotoView(
                photo: photo,
                width: width,
                height: height,
                contentMode: contentMode,
                cornerRadius: cornerRadius
            ).eraseToAnyView()
        } else {
            return placeholderView(cornerRadius: cornerRadius).eraseToAnyView()
        }
    }
    
    /// Standard placeholder for when no photo is available
    private static func placeholderView(cornerRadius: CGFloat) -> some View {
        Image(systemName: "fork.knife")
            .font(.system(size: 30))
            .foregroundColor(.gray)
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color.gray.opacity(0.1))
            .cornerRadius(cornerRadius)
    }
}

// Helper extension to erase View type
extension View {
    func eraseToAnyView() -> AnyView {
        AnyView(self)
    }
}
