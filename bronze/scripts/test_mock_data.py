import ollama
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from bronze.scripts.mock_data import generate_mock_reviews, MOCK_BUSINESSES

def check_model():
    """Verify llama3.2 is available before running tests."""
    print("Checking Ollama models...")
    print("-" * 50)
    
    models = ollama.list()
    available = [m.model for m in models.models]
    
    print(f"Available models: {available}")
    
    if not any("llama3.2" in m for m in available):
        print("❌ llama3.2 not found - run: ollama pull llama3.2")
        return False
    
    print("✅ llama3.2 is available\n")
    return True

if __name__ == "__main__":
    if not check_model():
        sys.exit(1)

    business = MOCK_BUSINESSES[0]

    print(f"Testing review generation for: {business['business_name']}")
    print("-" * 50)

    reviews = generate_mock_reviews(
        business_name=business["business_name"],
        business_type=business["business_type"],
        n=5
    )

    if not reviews:
        print("❌ No reviews generated - check Ollama is running")
    else:
        print(f"✅ Generated {len(reviews)} reviews\n")
        for i, review in enumerate(reviews, 1):
            print(f"Review {i}: {review['review_text']}")
            print()