import Foundation

enum LocationError: Error, LocalizedError {
    case locationNotAvailable
    case locationAccessDenied
    case locationServicesDisabled
    case timeout
    case unknown(Error?)
    
    var errorDescription: String? {
        switch self {
        case .locationNotAvailable:
            return "Location data is not available"
        case .locationAccessDenied:
            return "Location access was denied"
        case .locationServicesDisabled:
            return "Location services are disabled"
        case .timeout:
            return "Timed out waiting for location"
        case .unknown(let error):
            return "Unknown location error: \(error?.localizedDescription ?? "No details")"
        }
    }
}
