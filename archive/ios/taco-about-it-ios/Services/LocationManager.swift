import CoreLocation
import SwiftUI

class LocationManager: NSObject, ObservableObject, CLLocationManagerDelegate {
    private var locationManager = CLLocationManager()
    private var locationContinuation: CheckedContinuation<CLLocationCoordinate2D, Error>?
    
    @Published var errorMessage: String?
    
    override init() {
        super.init()
        locationManager.delegate = self
        locationManager.desiredAccuracy = kCLLocationAccuracyBest
    }
    
    // In LocationManager.swift
    func requestLocationAsync() async throws -> CLLocationCoordinate2D {
        let status = locationManager.authorizationStatus
        
        switch status {
        case .notDetermined:
            locationManager.requestWhenInUseAuthorization()
            // Wait for authorization status to change
            return try await withCheckedThrowingContinuation { continuation in
                self.locationContinuation = continuation
                // Add a timeout to prevent hanging indefinitely
                Task {
                    try? await Task.sleep(nanoseconds: 10_000_000_000) // 10 second timeout
                    if let cont = self.locationContinuation {
                        self.locationContinuation = nil
                        cont.resume(throwing: LocationError.timeout)
                    }
                }
            }
        case .denied, .restricted:
            throw LocationError.locationAccessDenied
        case .authorizedWhenInUse, .authorizedAlways:
            // Even with authorization, we need to check if location services are enabled
            if !CLLocationManager.locationServicesEnabled() {
                throw LocationError.locationServicesDisabled
            }
            
            return try await withCheckedThrowingContinuation { continuation in
                self.locationContinuation = continuation
                locationManager.startUpdatingLocation()
                
                // Add a timeout to prevent hanging if no location is received
                Task {
                    try? await Task.sleep(nanoseconds: 15_000_000_000) // 15 second timeout
                    if let cont = self.locationContinuation {
                        self.locationContinuation = nil
                        locationManager.stopUpdatingLocation()
                        cont.resume(throwing: LocationError.timeout)
                    }
                }
            }
        @unknown default:
            throw LocationError.unknown(nil)
        }
    }
    
    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        guard let location = locations.first else { return }
        locationManager.stopUpdatingLocation()
        locationContinuation?.resume(returning: location.coordinate)
        locationContinuation = nil
    }
    
    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        locationContinuation?.resume(throwing: LocationError.unknown(error))
        locationContinuation = nil
        errorMessage = "Failed to get location: \(error.localizedDescription)"
    }
}
