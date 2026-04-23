import SwiftUI

struct SplashScreen: View {
    // State to track if animation has completed
    @State private var isActive = false
    @State private var size = 0.8
    @State private var opacity = 0.5
    
    // Timer to automatically dismiss splash screen
    @State private var isTimerRunning = true
    
    // Custom font for the title - using correct PostScript name
    private let lieurFont = Font.custom("LIEUR-Regular", size: 50)
    
    var body: some View {
        if isActive {
            ContentView()
        } else {
            ZStack {
                // Background color
                Color(hex: "#FF7F50")
                    .ignoresSafeArea()
                
                VStack {
                    Spacer()
                    
                    // Logo and text
                    VStack(spacing: -5) {
                        Text("TACO")
                            .font(lieurFont)
                            .foregroundColor(.white)
                        
                        Text("BOUT IT")
                            .font(lieurFont)
                            .foregroundColor(.white)
                    }
                    .scaleEffect(size)
                    .opacity(opacity)
                    .onAppear {
                        // Start animation
                        withAnimation(.easeIn(duration: 1.2)) {
                            self.size = 1.0
                            self.opacity = 1.0
                        }
                    }
                    
                    Spacer()
                }
                .onAppear {
                    // Automatically transition to main view after delay
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2.5) {
                        if isTimerRunning {
                            withAnimation {
                                self.isActive = true
                            }
                        }
                    }
                }
            }
            .onTapGesture {
                // Allow user to skip animation by tapping
                isTimerRunning = false
                withAnimation {
                    self.isActive = true
                }
            }
        }
    }
}

// Preview
#Preview {
    SplashScreen()
}
