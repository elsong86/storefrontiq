import SwiftUI

struct Header: View {
    let title: [(character: String, color: Color)] = [
        ("T", .tacoRose),
        ("A", .tacoEmerald),
        ("C", .tacoYellow),
        ("O", .tacoOrange),
        ("A", .tacoRose),
        ("B", .tacoEmerald),
        ("O", .tacoYellow),
        ("U", .tacoOrange),
        ("T", .tacoRose),
        ("I", .tacoEmerald),
        ("T", .tacoYellow)
    ]

    var body: some View {
        HStack {
            Image("logo")
                .resizable()
                .scaledToFit()
                .frame(height: 40)

            HStack(spacing: 0) {
                ForEach(Array(zip(title.indices, title)), id: \.0) { _, item in
                    Text(item.character)
                        .foregroundColor(item.color)
                        .font(AppFonts.hustlersDemo30)
                }
            }

            Spacer()

            Image(systemName: "line.3.horizontal")
                .resizable()
                .scaledToFit()
                .frame(width: 24, height: 24)
        }
        .padding(.horizontal)
        .padding(.vertical, 8)
        .background(Color.white.opacity(0.9))
        .shadow(color: .gray.opacity(0.5), radius: 5, x: 0, y: 2)
        .frame(maxWidth: .infinity)
    }
}

#Preview {
    Header()
}
