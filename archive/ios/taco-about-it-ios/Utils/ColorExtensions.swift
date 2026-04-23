//
//  ColorExtensions.swift
//  taco-about-it-ios
//
//  Created by Ellis Song on 11/28/24.
//
import SwiftUI

extension Color {
    init(hex: String) {
        let hex = hex.trimmingCharacters(in: CharacterSet.alphanumerics.inverted)
        var int: UInt64 = 0
        Scanner(string: hex).scanHexInt64(&int)
        let r, g, b: Double
        switch hex.count {
        case 3: // RGB (12-bit)
            (r, g, b) = (
                Double((int >> 8) * 17) / 255,
                Double((int >> 4 & 0xF) * 17) / 255,
                Double((int & 0xF) * 17) / 255
            )
        case 6: // RGB (24-bit)
            (r, g, b) = (
                Double((int >> 16) & 0xFF) / 255,
                Double((int >> 8) & 0xFF) / 255,
                Double(int & 0xFF) / 255
            )
        default:
            (r, g, b) = (1, 1, 1) // Default to white for invalid HEX
        }
        self.init(red: r, green: g, blue: b)
    }
    
    static let tacoRose = Color(hex: "#9F1239")    
    static let tacoEmerald = Color(hex: "#065F46")
    static let tacoYellow = Color(hex: "#D97706")
    static let tacoOrange = Color(hex: "#C2410C")
}
