import SwiftUI

struct ContentView: View {
    @State private var searchText: String = ""
    @State private var destination: Destination?
    @State private var isSearchLoading = false
    @State private var viewModel = ContentViewModel(useMockData: false)

    

    
    enum Destination: Hashable {
            case places(location: GeoLocation, places: [Place])
            
            // Add these functions to conform to Hashable
            func hash(into hasher: inout Hasher) {
                switch self {
                case .places(let location, let places):
                    hasher.combine(location)
                    hasher.combine(places.map { $0.id })  // Hash the IDs of places
                }
            }
            
            static func == (lhs: Destination, rhs: Destination) -> Bool {
                switch (lhs, rhs) {
                case (.places(let loc1, let places1), .places(let loc2, let places2)):
                    return loc1 == loc2 && places1.map({ $0.id }) == places2.map({ $0.id })
                }
            }
        }

    let tacoSpotCharacters: [(character: String, color: Color)] = [
        ("T", .tacoRose),
        ("A", .tacoEmerald),
        ("C", .tacoYellow),
        ("O", .tacoOrange),
        ("S", .tacoRose),
        ("P", .tacoEmerald),
        ("O", .tacoYellow),
        ("T", .tacoRose)
    ]

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                // Header
                Header()
                    .padding(.bottom, 16)

                // Main Content
                ScrollView {
                    VStack(spacing: 16) {
                        Spacer()
                            .frame(height: 40)

                        // Title Section
                        TitleSectionView(tacoSpotCharacters: tacoSpotCharacters)

                        // Instruction Text
                        Text("Share or enter your location to get started")
                            .foregroundColor(.black)
                            .multilineTextAlignment(.center)
                            .padding(.horizontal, 16)

                        Divider()

                        // Action Buttons
                        ActionButtonsView(
                            viewModel: viewModel,
                            searchText: $searchText,
                            destination: $destination
                        )
                        .padding()
                    }
                }
            }
            .background(Color.white)
            .navigationDestination(item: $destination) { destination in
                switch destination {
                case .places(let location, let places):
                    let placesViewModel = PlacesViewModel(prefetchedPlaces: places)
                    PlacesListView(
                        viewModel: placesViewModel,
                        location: location
                    )
                    .onDisappear {
                        viewModel.resetLocation()
                    }
                }
            }
            .alert("Error", isPresented: Binding(
                get: { viewModel.errorMessage != nil },
                set: { if !$0 { viewModel.errorMessage = nil } }
            )) {
                Button("OK") {
                    viewModel.errorMessage = nil
                }
            } message: {
                if let errorMessage = viewModel.errorMessage {
                    Text(errorMessage)
                }
            }
        }
    }
}

#Preview {
    ContentView()
        .environmentObject(ContentViewModel.preview)  
}
