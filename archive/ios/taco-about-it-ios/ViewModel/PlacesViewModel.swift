import SwiftUI
import Observation

@Observable @MainActor
class PlacesViewModel {
    var places: [Place] = []
    var errorMessage: String? = nil

    init(prefetchedPlaces: [Place] = []) {
        self.places = prefetchedPlaces
    }
}
