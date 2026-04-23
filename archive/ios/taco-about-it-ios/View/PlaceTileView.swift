import SwiftUI

struct PlaceTileView: View {
    let place: Place
    @State private var photoURL: URL? = nil
    @State private var isLoadingPhoto = false
    
    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            // Image
            restaurantImageView
                .frame(width: 80, height: 80)
                .cornerRadius(8)
            
            // Content
            VStack(alignment: .leading, spacing: 6) {
                // Restaurant Name
                Text(place.displayNameText)
                    .font(.headline)
                    .foregroundColor(.primary)
                    .lineLimit(1)
                
                // Address - with multiple lines allowed
                if let address = place.formattedAddress {
                    Text(address)
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                        .lineLimit(3) // Allow up to 3 lines
                        .fixedSize(horizontal: false, vertical: true) // Important for proper text wrapping
                }
                
                // Ratings
                if let rating = place.rating {
                    HStack(spacing: 2) {
                        // Stars
                        ForEach(1...5, id: \.self) { index in
                            Image(systemName: index <= Int(rating) ? "star.fill" : "star")
                                .foregroundColor(.yellow)
                                .font(.caption)
                        }
                        
                        Text(String(format: "%.1f", rating))
                            .font(.caption2)
                            .foregroundColor(.secondary)
                            .padding(.leading, 2)
                        
                        if let reviews = place.userRatingCount {
                            Text("(\(reviews))")
                                .font(.caption2)
                                .foregroundColor(.secondary)
                        }
                        
                        Spacer(minLength: 0)
                    }
                }
            }
            
            Spacer(minLength: 0)
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(Color(.systemBackground))
                .shadow(color: .black.opacity(0.1), radius: 2, x: 0, y: 2)
        )
        .task {
            if photoURL == nil {
                await loadPhoto()
            }
        }
    }
    
    // Restaurant image view
    private var restaurantImageView: some View {
        Group {
            if isLoadingPhoto {
                ProgressView()
                    .frame(width: 80, height: 80)
                    .background(Color.gray.opacity(0.1))
            } else if let photoURL = photoURL {
                AsyncImage(url: photoURL) { phase in
                    switch phase {
                    case .empty:
                        ProgressView()
                            .frame(width: 80, height: 80)
                            .background(Color.gray.opacity(0.1))
                    case .success(let image):
                        image
                            .resizable()
                            .aspectRatio(contentMode: .fill)
                    case .failure:
                        Image(systemName: "fork.knife")
                            .font(.system(size: 30))
                            .foregroundColor(.gray)
                            .frame(width: 80, height: 80)
                            .background(Color.gray.opacity(0.1))
                    @unknown default:
                        Image(systemName: "fork.knife")
                            .font(.system(size: 30))
                            .foregroundColor(.gray)
                            .frame(width: 80, height: 80)
                            .background(Color.gray.opacity(0.1))
                    }
                }
            } else {
                Image(systemName: "fork.knife")
                    .font(.system(size: 30))
                    .foregroundColor(.gray)
                    .frame(width: 80, height: 80)
                    .background(Color.gray.opacity(0.1))
            }
        }
    }
    
    // Function to load the photo with retry and error handling
    private func loadPhoto() async {
        guard let photo = place.primaryPhoto, !isLoadingPhoto, photoURL == nil else {
            return
        }
        
        isLoadingPhoto = true
        defer { isLoadingPhoto = false }
        
        do {
            photoURL = try await PlacesService.shared.fetchPhotoURL(
                for: photo,
                maxWidth: 160,
                maxHeight: 160
            )
        } catch {
            print("Photo loading error: \(error.localizedDescription)")
        }
    }
}
