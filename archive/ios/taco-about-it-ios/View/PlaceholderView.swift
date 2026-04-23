import SwiftUI

struct PlaceholderView: View {
    var location: GeoLocation?
    var searchString: String?
    var onDisappear: () -> Void // Closure to reset location
    
    var body: some View {
        VStack {
            if let location = location {
                Text("Location: \(location.latitude), \(location.longitude)")
                    .font(.title)
                    .padding()
            } else if let searchString = searchString {
                Text("Search Query: \(searchString)")
                    .font(.title)
                    .padding()
            } else {
                Text("No data available.")
                    .font(.title)
                    .padding()
            }
        }
        .onDisappear {
            onDisappear() // Call the closure when the view disappears
        }
    }
}

#Preview {
    PlaceholderView(location: GeoLocation(latitude: 37.7749, longitude: -122.4194), searchString: nil, onDisappear: {})
}
