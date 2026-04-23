import Foundation

struct Review: Codable {
    let reviewText: String
    
    enum CodingKeys: String, CodingKey {
        case reviewText = "review_text"
    }
}

struct ReviewAnalysisResponse: Codable {
    let averageSentiment: Double
    let reviews: [Review]
    let source: String
    
    enum CodingKeys: String, CodingKey {
        case averageSentiment = "average_sentiment"
        case reviews
        case source
    }
}
