import Foundation
import KeychainAccess

// Define NetworkError outside or inside the class, but not inside a method.
// Placing it inside makes it SessionManager.NetworkError
enum NetworkError: Error {
    case invalidURL
    case invalidResponse
    case httpError(statusCode: Int)
    case decodingError(Error) // Added specific case for decoding issues
    case sessionCreationFailed(Error) // General failure case
}

class SessionManager {
    static let shared = SessionManager()
    
    private let keychain = Keychain(service: "com.tacoaboutit.app") // Ensure this matches your app's identifier
    // --- Make sure these are correct for your setup ---
    private let baseURL = "https://api.tacoaboutit.app"
    private let appSecret: String = ConfigurationManager.shared.getAPIKey()
    // ----------------------------------------------------
    
    private(set) var sessionToken: String?
    private(set) var sessionExpiry: Date?
    
    private init() {
        // Try to load session from keychain on init
        loadSession()
        print("SessionManager initialized. Session loaded: \(sessionToken != nil), Expires: \(sessionExpiry?.description ?? "N/A")")
    }
    
    // Check if we have a valid session
    var hasValidSession: Bool {
        guard sessionToken != nil, let expiry = sessionExpiry else {
            return false
        }
        // Check if token is not expired (using a buffer, e.g., 1 hour = 3600 seconds)
        let isValid = expiry > Date().addingTimeInterval(3600)
        if !isValid {
             print("Session token is expired or nearing expiry.")
        }
        return isValid
    }
    
    // Create a new session by calling the backend
    func createSession() async throws -> String {
        guard let url = URL(string: "\(baseURL)/create-session") else {
            throw NetworkError.invalidURL
        }
        
        print("Attempting to create new session...")
        
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.addValue("application/json", forHTTPHeaderField: "Content-Type")
        request.addValue(appSecret, forHTTPHeaderField: "X-API-Key") // Sending the static app secret
        
        print("Creating session with URL: \(url.absoluteString)")
        print("Using app secret: \(appSecret.prefix(5))...")
        
        do {
            // --- Network Call ---
            let (data, response) = try await URLSession.shared.data(for: request)
            
            // --- Response Validation ---
            guard let httpResponse = response as? HTTPURLResponse else {
                print("Invalid response type received.")
                throw NetworkError.invalidResponse
            }
            print("Create session HTTP status: \(httpResponse.statusCode)")

            // --- Log Raw Response ---
            if let jsonString = String(data: data, encoding: .utf8) {
                print("--- RAW JSON Response from /create-session ---")
                print(jsonString)
                print("----------------------------------------------")
            } else {
                print("--- Failed to convert response data to UTF8 string ---")
                // Still might be valid data, just not UTF8 text, but unlikely for JSON API
            }

            // --- Handle Non-Success Status Codes ---
            guard (200..<300).contains(httpResponse.statusCode) else {
                var errorDetail = "Unknown error"
                if let responseString = String(data: data, encoding: .utf8) {
                     errorDetail = responseString
                }
                print("Create session failed with HTTP status \(httpResponse.statusCode). Response: \(errorDetail)")
                throw NetworkError.httpError(statusCode: httpResponse.statusCode)
            }

            // --- Define Expected Response Structure ---
            struct SessionResponse: Decodable {
                let token: String
                let expiresAt: Date
            }
            
            // --- Prepare Decoder ---
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .custom({ (decoder) -> Date in
                            // 1. Get the container holding the single date string value
                            let container = try decoder.singleValueContainer()
                            // 2. Decode the string from the container
                            let dateString = try container.decode(String.self)

                            // 3. Create and configure the ISO8601 formatter
                            let formatter = ISO8601DateFormatter()
                            formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds] // Handle fractional seconds

                            // 4. Attempt to convert the string to a Date
                            if let date = formatter.date(from: dateString) {
                                return date // Success! Return the parsed Date
                            } else {
                                // 5. If conversion fails, throw a dataCorrupted error
                                throw DecodingError.dataCorruptedError(in: container,
                                                                       debugDescription: "Cannot decode date string \(dateString)")
                            }
                        })

            // --- Attempt Decoding ---
            do {
                let sessionResponse = try decoder.decode(SessionResponse.self, from: data)
                
                print("Successfully decoded session response.")
                print("Token received: \(sessionResponse.token.prefix(10))...")
                print("Expires at: \(sessionResponse.expiresAt)")

                // --- Store Session Data ---
                self.sessionToken = sessionResponse.token
                self.sessionExpiry = sessionResponse.expiresAt
                
                // --- Save to Keychain ---
                try self.saveSession()
                print("Session saved successfully to keychain.")

                return sessionResponse.token

            } catch let decodeError {
                // --- Handle Decoding Errors ---
                print("--- DECODING FAILED ---")
                print("Failed to decode SessionResponse. Error: \(decodeError)")
                if let decodingError = decodeError as? DecodingError {
                    print("Decoding Error Context: \(decodingError)")
                }
                print("-----------------------")
                // Throw a specific decoding error
                throw NetworkError.decodingError(decodeError)
            }
        } catch let error where !(error is NetworkError) {
             // Catch other potential errors (e.g., URLSession errors like no network)
             print("Create session failed with underlying error: \(error.localizedDescription)")
             throw NetworkError.sessionCreationFailed(error)
        }
        // Note: Errors explicitly thrown as NetworkError above will propagate directly.
    }
    
    // Ensure we have a valid session, creating one if needed
    func ensureValidSession() async throws -> String {
        if hasValidSession, let token = sessionToken {
            print("Using existing valid session token.")
            return token
        }
        
        print("No valid session found or session expired. Creating new session...")
        // Clear potentially expired/invalid data before creating a new one
        if sessionToken != nil || sessionExpiry != nil {
             try? clearSession() // Attempt to clear, ignore error if it fails
        }
        // Create new session
        return try await createSession()
    }
    
    // Save session to keychain
    private func saveSession() throws {
        guard let token = sessionToken, let expiry = sessionExpiry else {
            print("SaveSession failed: Token or Expiry is nil.")
            return // Nothing to save
        }
        
        do {
            try keychain.set(token, key: "sessionToken")
            // Store expiry as TimeInterval (Double) string
            try keychain.set(expiry.timeIntervalSince1970.description, key: "sessionExpiry")
            print("Session token and expiry saved to keychain.")
        } catch {
            print("Failed to save session to keychain: \(error)")
            throw error // Re-throw the keychain error
        }
    }
    
    // Load session from keychain
    private func loadSession() {
        do {
            if let token = try keychain.getString("sessionToken"),
               let expiryString = try keychain.getString("sessionExpiry"),
               let expiryInterval = Double(expiryString) {
                
                self.sessionToken = token
                self.sessionExpiry = Date(timeIntervalSince1970: expiryInterval)
                print("Session successfully loaded from keychain.")
            } else {
                 print("No session found in keychain or data invalid.")
                 // Ensure properties are nil if loading fails partially
                 self.sessionToken = nil
                 self.sessionExpiry = nil
            }
        } catch {
            print("Failed to load session from keychain: \(error)")
            // Ensure properties are nil if keychain access throws
            self.sessionToken = nil
            self.sessionExpiry = nil
        }
    }
    
    // Clear session (for troubleshooting or logout)
    func clearSession() throws {
        print("Clearing session data...")
        sessionToken = nil
        sessionExpiry = nil
        do {
            try keychain.remove("sessionToken")
            try keychain.remove("sessionExpiry")
            print("Session data removed from keychain.")
        } catch {
            print("Failed to remove session data from keychain: \(error)")
            throw error // Re-throw the keychain error
        }
    }
}
