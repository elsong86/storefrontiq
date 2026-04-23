import SwiftUI

struct TitleSectionView: View {
    let tacoSpotCharacters: [(character: String, color: Color)]
    
    var body: some View {
        VStack(spacing: 16) {
            Text("Anywhere, Anytime")
                .font(AppFonts.thirstyReg20)
                .foregroundColor(.black)
                .multilineTextAlignment(.center)
            
            Text("Find Your New")
                .font(AppFonts.brothers30)
                .foregroundColor(.black)
                .multilineTextAlignment(.center)
            
            Text("Favorite")
                .font(AppFonts.brothers30)
                .foregroundColor(.black)
                .multilineTextAlignment(.center)
            
            HStack(spacing: 0) {
                ForEach(Array(tacoSpotCharacters.enumerated()), id: \.offset) { index, item in
                    Text(item.character)
                        .foregroundColor(item.color)
                        .font(AppFonts.brothers50)
                }
            }
        }
        .frame(maxWidth: .infinity)
    }
}

#Preview {
    // Sample Preview with mock characters and colors
    TitleSectionView(tacoSpotCharacters: [
        ("T", Color(hex: "#9F1239")), // Rose 800
        ("A", Color(hex: "#065F46")), // Emerald 800
        ("C", Color(hex: "#D97706")), // Yellow 600
        ("O", Color(hex: "#C2410C")), // Orange 700
        ("S", Color(hex: "#9F1239")), // Rose 800
        ("P", Color(hex: "#065F46")), // Emerald 800
        ("O", Color(hex: "#D97706")), // Yellow 600
        ("T", Color(hex: "#9F1239"))  // Rose 800
    ])
}
