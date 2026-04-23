import Foundation

struct ConfigurationManager {
    static let shared = ConfigurationManager()
    
    private let plistName = "APIConfig"
    
    private init() {}
    
    func getAPIKey() -> String {
        guard let path = Bundle.main.path(forResource: plistName, ofType: "plist"),
              let xml = FileManager.default.contents(atPath: path),
              let plist = try? PropertyListSerialization.propertyList(from: xml, options: .mutableContainersAndLeaves, format: nil) as? [String: Any],
              let apiKey = plist["API_KEY"] as? String else {
            fatalError("Failed to load API key from plist")
        }
        return apiKey
    }
}
