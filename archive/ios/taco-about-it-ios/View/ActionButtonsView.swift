import SwiftUI

struct ActionButtonsView: View {
    @ObservedObject var viewModel: ContentViewModel
    @Binding var searchText: String
    @Binding var destination: ContentView.Destination?
    @State private var isLoading = false
    @State private var isSearchLoading = false
    
    var body: some View {
        VStack {
            Button(action: {
                Task {
                    isLoading = true
                    do {
                        let (location, places) = try await viewModel.requestLocationAndFetchPlaces()
                        destination = .places(location: location, places: places)
                    } catch let error as LocationError {
                        // More specific error handling
                        switch error {
                        case .timeout:
                            viewModel.errorMessage = "Location request timed out. Please try again."
                        case .locationAccessDenied:
                            viewModel.errorMessage = "Please enable location access in Settings to use this feature."
                        case .locationServicesDisabled:
                            viewModel.errorMessage = "Please enable location services on your device to use this feature."
                        default:
                            viewModel.errorMessage = error.localizedDescription
                        }
                    } catch {
                        viewModel.errorMessage = error.localizedDescription
                    }
                    isLoading = false
                }
            }) {
                HStack {
                    if isLoading {
                        ProgressView()
                            .progressViewStyle(CircularProgressViewStyle(tint: .white))
                            .padding(.trailing, 8)
                    }
                    Label("Share Location", systemImage: "location.fill")
                }
                .frame(maxWidth: .infinity)
            }
            .disabled(isLoading)
            .padding()
            .background(Color.blue)
            .foregroundColor(.white)
            .cornerRadius(10)
            .shadow(color: .black.opacity(0.5), radius: 5, x: 2, y: 2)
            
            Text("OR")
                .foregroundColor(.black)
                .padding(.vertical, 8)
            
            SearchBarView(
                searchText: $searchText,
                isLoading: isSearchLoading,
                onSearch: { text in
                    Task {
                        isSearchLoading = true
                        do {
                            let (location, places) = try await viewModel.handleSearch(address: text)
                            destination = .places(location: location, places: places)
                        } catch {
                            viewModel.errorMessage = error.localizedDescription
                        }
                        isSearchLoading = false
                    }
                }
            )
            .frame(maxWidth: .infinity)
            .padding(.horizontal, 16)
            .padding(.vertical, 8)
            .background(Color.white)
            .cornerRadius(8)
            .shadow(color: .gray.opacity(0.5), radius: 5, x: 2, y: 2)
        }
    }
}

#Preview {
    ActionButtonsView(
        viewModel: ContentViewModel(),
        searchText: .constant(""),
        destination: .constant(nil)
    )
}
