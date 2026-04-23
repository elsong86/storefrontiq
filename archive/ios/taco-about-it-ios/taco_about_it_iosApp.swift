import SwiftUI

@main
struct taco_about_it_iosApp: App {
    @Environment(\.scenePhase) private var scenePhase
    @State private var isSessionInitialized = false
    @State private var sessionError: Error? = nil
    
    var body: some Scene {
        WindowGroup {
            Group {
                if isSessionInitialized {
                    ContentView()
                } else {
                    SplashScreen()
                        .task {
                            // Initialize session during splash screen
                            do {
                                let token = try await SessionManager.shared.ensureValidSession()
                                print("Session initialized successfully with token: \(token.prefix(10))...")
                                isSessionInitialized = true
                            } catch {
                                print("Failed to initialize session: \(error.localizedDescription)")
                                sessionError = error
                                // Still proceed to the app, but mark that we had an error
                                isSessionInitialized = true
                            }
                        }
                }
            }
            .alert("Session Error", isPresented: Binding(
                get: { sessionError != nil },
                set: { if !$0 { sessionError = nil } }
            )) {
                Button("Retry") {
                    Task {
                        do {
                            let token = try await SessionManager.shared.ensureValidSession()
                            print("Session initialized successfully with token: \(token.prefix(10))...")
                            sessionError = nil
                        } catch {
                            print("Retry failed: \(error.localizedDescription)")
                            sessionError = error
                        }
                    }
                }
                Button("Continue") {
                    sessionError = nil
                }
            } message: {
                if let error = sessionError {
                    Text("Failed to initialize session: \(error.localizedDescription)")
                }
            }
        }
        .onChange(of: scenePhase) { oldPhase, newPhase in
            if newPhase == .background {
                // App moved to background, perform maintenance
                Task {
                    await DiskCacheService.shared.performMaintenance()
                }
            }
        }
    }
}

// Preview
#Preview {
    SplashScreen()
}
