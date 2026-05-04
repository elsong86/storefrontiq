import ollama
import json

MOCK_BUSINESSES = [
    {
        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
        "business_name": "Mario's Pizzeria",
        "business_type": "pizza restaurant",
        "address": "123 Main St, San Francisco, CA"
    },
    {
        "place_id": "ChIJa8K5XnmmI4gRTUwGh6e7AZU",
        "business_name": "Sakura Sushi",
        "business_type": "sushi restaurant",
        "address": "456 Market St, San Francisco, CA"
    },
    {
        "place_id": "ChIJ7cv00DwsDogRAMDACha2mQ0",
        "business_name": "The Coffee Spot",
        "business_type": "coffee shop",
        "address": "789 Valencia St, San Francisco, CA"
    },
    {
        "place_id": "ChIJO3qMc8gDdkgRu8siOXHqt7w",
        "business_name": "Burger Republic",
        "business_type": "burger restaurant",
        "address": "321 Castro St, San Francisco, CA"
    },
    {
        "place_id": "ChIJ2eUgeAK6j4ARbn5u_wAGqWA",
        "business_name": "Green Bowl",
        "business_type": "healthy food restaurant",
        "address": "654 Hayes St, San Francisco, CA"
    },
]

def generate_batch(business_name: str, business_type: str, count: int, sentiment: str) -> list[str]:
    """Generate a single small batch of reviews using structured JSON output."""
    prompt = f"""Generate exactly {count} Google Maps reviews for a {business_type} called "{business_name}" in San Francisco.

- Sentiment: {sentiment}
- Max 30 words per review
- Sound like real people wrote them
- Mention specific details like food items, service, or prices

Return a JSON object with a single key "reviews" containing an array of {count} review strings."""

    response = ollama.generate(
        model="llama3.2",
        prompt=prompt,
        format="json",  # forces valid JSON output
        options={
            "temperature": 0.9,
            "num_predict": 512
        }
    )

    raw = response["response"].strip()
    parsed = json.loads(raw)
    
    # extract reviews array from the object
    return parsed.get("reviews", [])

def generate_mock_reviews(business_name: str, business_type: str, n: int = 30) -> list[dict]:
    """Generate reviews in small batches of 5 to avoid truncation."""

    all_reviews = []
    batch_size = 5

    n_positive = int(n * 0.5)
    n_neutral = int(n * 0.3)
    n_negative = n - n_positive - n_neutral

    batches = [
        (n_positive, "positive"),
        (n_neutral, "neutral"),
        (n_negative, "negative"),
    ]

    for total_count, sentiment in batches:
        # break each sentiment into chunks of batch_size
        remaining = total_count
        while remaining > 0:
            chunk = min(batch_size, remaining)
            print(f"🤖 Generating {chunk} {sentiment} reviews for {business_name}...")

            try:
                reviews = generate_batch(business_name, business_type, chunk, sentiment)
                all_reviews.extend([{"review_text": r} for r in reviews])
                print(f"✅ Got {len(reviews)} {sentiment} reviews")
                remaining -= chunk

            except json.JSONDecodeError as e:
                print(f"⚠️  Parse failed for {business_name} ({sentiment} batch): {e}")
                remaining -= chunk  # skip failed batch and continue
                continue

    print(f"✅ Total reviews generated for {business_name}: {len(all_reviews)}")
    return all_reviews