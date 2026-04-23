import SwiftUI

struct SearchBarView: View {
    @Binding var searchText: String
    let isLoading: Bool
    var onSearch: (String) -> Void

    var body: some View {
        HStack(spacing: 8) {
            TextField("Enter Address", text: $searchText)
                .padding(.horizontal, 8)
                .frame(height: 40)
                .background(Color.white)
                .cornerRadius(8)

            Button(action: {
                onSearch(searchText)
            }) {
                HStack {
                    if isLoading {
                        ProgressView()
                            .progressViewStyle(CircularProgressViewStyle(tint: .white))
                            .padding(.trailing, 4)
                    }
                    Text("Search")
                }
                .padding(.horizontal, 16)
                .frame(height: 40)
            }
            .disabled(isLoading)
            .background(Color.blue)
            .foregroundColor(.white)
            .cornerRadius(8)
        }
        .frame(maxWidth: UIScreen.main.bounds.width * 0.9)
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
        .shadow(color: .gray.opacity(0.5), radius: 5, x: 2, y: 2)
    }
}

#Preview {
    SearchBarView(
        searchText: .constant("Test Address"),
        isLoading: false,
        onSearch: { searchText in
            print("Search triggered for: \(searchText)")
        }
    )
    .padding()
}
