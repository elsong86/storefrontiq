import XCTest
@testable import taco_about_it_ios

class URLProtocolMock: URLProtocol {
    static var requestHandler: ((URLRequest) throws -> (HTTPURLResponse, Data))?
    
    override class func canInit(with request: URLRequest) -> Bool {
        return true
    }
    
    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        return request
    }
    
    override func startLoading() {
        guard let handler = URLProtocolMock.requestHandler else {
            XCTFail("Handler is unavailable.")
            return
        }
        
        do {
            let (response, data) = try handler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }
    
    override func stopLoading() {}
}

final class PlacesServiceTests: XCTestCase {
    var sut: PlacesService!
    var mockURLSession: URLSession!
    
    override func setUp() {
        super.setUp()
        
        // Configure mock URLSession
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [URLProtocolMock.self]
        mockURLSession = URLSession(configuration: configuration)
        
        sut = PlacesService(urlSession: mockURLSession)
    }
    
    override func tearDown() {
        sut = nil
        mockURLSession = nil
        URLProtocolMock.requestHandler = nil
        super.tearDown()
    }
    
    func testFetchPlacesWithValidLocation() async throws {
        // Given
        let location = GeoLocation(latitude: 37.7749, longitude: -122.4194)
        
        // Create a PlacesResponse object
        let mockPlacesResponse = PlacesResponse(places: [
            Place(
                id: "test-id",
                displayName: DisplayName(text: "Test Taco Place"),
                formattedAddress: "123 Test St",
                rating: 4.5,
                userRatingCount: 100,
                photos: []
            )
        ])
        
        // Encode the response
        let mockData = try JSONEncoder().encode(mockPlacesResponse)
        
        let mockHTTPResponse = HTTPURLResponse(
            url: URL(string: "https://your-backend-url.com/places")!,
            statusCode: 200,
            httpVersion: nil,
            headerFields: ["Content-Type": "application/json"]
        )!
        
        // Set up the mock handler
        URLProtocolMock.requestHandler = { request in
            return (mockHTTPResponse, mockData)
        }
        
        // When
        let places = try await sut.fetchPlaces(location: location)
        
        // Then
        XCTAssertEqual(places.count, 1)
        XCTAssertEqual(places[0].id, "test-id")
        XCTAssertEqual(places[0].displayNameText, "Test Taco Place")
    }
}
