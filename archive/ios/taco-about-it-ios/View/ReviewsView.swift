import SwiftUI

struct ReviewsView: View {
    let reviews: [Review]
    let averageSentiment: Double
    
    private var sentimentColor: Color {
        switch averageSentiment {
        case 8...: return .green
        case 6..<8: return .yellow
        default: return .orange
        }
    }
    
    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            // Sentiment Score Section
            VStack(alignment: .leading, spacing: 8) {
                Text("Overall Sentiment")
                    .font(.headline)
                    .foregroundColor(.secondary)
                
                HStack(alignment: .firstTextBaseline) {
                    Text(String(format: "%.1f", averageSentiment))
                        .font(.system(.title, design: .rounded))
                        .fontWeight(.bold)
                        .foregroundColor(sentimentColor)
                    
                    Text("/10")
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                }
            }
            .padding(.horizontal)
            .padding(.vertical, 12)
            .background(Color(.systemBackground))
            .cornerRadius(10)
            .shadow(color: .black.opacity(0.05), radius: 2, x: 0, y: 1)
            
            // Reviews Section
            VStack(alignment: .leading, spacing: 16) {
                Text("Reviews")
                    .font(.headline)
                
                if reviews.isEmpty {
                    Text("No reviews yet")
                        .font(.body)
                        .foregroundColor(.secondary)
                } else {
                    VStack(alignment: .leading, spacing: 16) {
                        ForEach(reviews.indices, id: \.self) { index in
                            ReviewCell(review: reviews[index])
                            
                            if index < reviews.count - 1 {
                                Divider()
                            }
                        }
                    }
                }
            }
            .padding(.horizontal)
            .padding(.vertical, 12)
            .background(Color(.systemBackground))
            .cornerRadius(10)
            .shadow(color: .black.opacity(0.05), radius: 2, x: 0, y: 1)
        }
        .padding(.horizontal)
    }
}

struct ReviewCell: View {
    let review: Review
    
    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(review.reviewText)
                .font(.body)
                .fixedSize(horizontal: false, vertical: true)
        }
    }
}

#Preview {
    ScrollView {
        ReviewsView(
            reviews: Review.mockReviews,  
            averageSentiment: 8.5
        )
    }
    .background(Color(.systemGroupedBackground))
}
