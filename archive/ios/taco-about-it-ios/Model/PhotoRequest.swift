import Foundation

struct PhotoRequest: Codable {
    let photoName: String
    let maxHeight: Int?
    let maxWidth: Int?
    
    enum CodingKeys: String, CodingKey {
        case photoName = "photo_name"
        case maxHeight = "max_height"
        case maxWidth = "max_width"
    }
}

struct PhotoResponse: Codable {
    let url: String
}
