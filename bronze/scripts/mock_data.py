import ollama
import json
import random

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

def generate_mock_reviews(business_name: str, business_type: str, n: int = 30) -> list[dict]:
    """Use local Ollama llama3.2 to generate realistic mock reviews."""

    # distribute sentiment realistically
    n_positive = int(n * 0.5)
    n_neutral = int(n * 0.3)
    n_negative = n - n_positive - n_neutral

    prompt = f"""Generate {n} realistic Google Maps customer reviews for a {business_type} called "{business_name}" located in San Francisco.

Requirements:
- {n_positive} positive reviews (happy customers)
- {n_neutral} neutral reviews (mixed feelings)
- {n_negative} negative reviews (unhappy customers)
- Each review should sound like a real person wrote it
- Vary the length and writing style (some short, some detailed)
- Reference specific things like food, service, atmosphere, wait times, prices
- Do not number the reviews or add labels like "Positive:" or "Negative:"

Return ONLY a JSON array of strings, one string per review, no other text.
Example format: ["review one here", "review two here"]"""

    print(f"🤖 Generating {n} reviews for {business_name} via Ollama...")

    response = ollama.generate(
        model="llama3.2",
        prompt=prompt,
        options={"temperature": 0.9}  # higher temp = more varied output
    )

    raw = response["response"].strip()

    # parse the JSON array from the response
    try:
        # handle cases where model wraps response in markdown code blocks
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        reviews = json.loads(raw)
        print(f"✅ Generated {len(reviews)} reviews for {business_name}")
        return [{"review_text": r} for r in reviews]

    except json.JSONDecodeError as e:
        print(f"⚠️  Failed to parse Ollama response for {business_name}: {e}")
        print(f"Raw response: {raw[:200]}...")
        # fall back to empty list - ingest.py handles this gracefully
        return []