import SwiftUI

struct PlacesListView: View {
    @State private var viewModel: PlacesViewModel
    let location: GeoLocation
    
    init(viewModel: PlacesViewModel, location: GeoLocation) {
        _viewModel = State(wrappedValue: viewModel)
        self.location = location
    }

    var body: some View {
        Group {
            if viewModel.places.isEmpty {
                Text("No places found")
                    .foregroundColor(.gray)
            } else {
                ScrollView {
                    LazyVStack(spacing: 12) {
                        ForEach(viewModel.places) { place in
                            NavigationLink {
                                PlaceView(place: place)
                            } label: {
                                PlaceTileView(place: place)
                            }
                            .buttonStyle(PlainButtonStyle())
                        }
                    }
                    .padding(.vertical)
                }
            }
        }
        .navigationTitle("Nearby Places")
        .onAppear {
            // Launch task from non-async context
            Task {
                await preloadImagesForVisibleRows()
            }
        }
        // Alternative approach using task modifier
        .task {
            // This is automatically async
            await preloadImagesForVisibleRows()
        }
    }
    
    // This function is async
    private func preloadImagesForVisibleRows() async {
        // Get visible and soon-to-be-visible places based on scroll position
        let visiblePlaces = viewModel.places.prefix(10) // Adjust based on your UI
        await ImageCacheService.shared.prefetchImagesForPlaces(Array(visiblePlaces))
    }
}

#Preview {
    NavigationView {
        PlacesListView(
            viewModel: PlacesViewModel.preview,
            location: MockData.location
        )
    }
}
