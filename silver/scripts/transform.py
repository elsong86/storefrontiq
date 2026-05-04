import re
import pyarrow as pa
import pandas as pd
import ollama
import json 
from deltalake import DeltaTable, write_deltalake
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv
from datetime import datetime, timezone
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from silver.schemas.review_schema import silver_review_schema
from bronze.scripts.mock_data import MOCK_BUSINESSES

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

storage_options = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": AWS_REGION,
}

analyzer = SentimentIntensityAnalyzer()


def clean_review_text(text: str) -> str:
    """Strip LLM artifacts and noise from raw review text."""
    if not text or not text.strip():
        return ""
    
    cleaned = text.strip()
    
    # Strip leading star ratings like "5/5 stars - " or "4.5/5 stars -"
    cleaned = re.sub(r"^\d+(?:\.\d+)?/\d+\s*stars?\s*[-–—:]\s*", "", cleaned, flags=re.IGNORECASE)
    
    # Strip leading rating-only prefixes like "5 stars - "
    cleaned = re.sub(r"^\d+\s*stars?\s*[-–—:]\s*", "", cleaned, flags=re.IGNORECASE)
    
    # Strip leading "1/5 -" style ratings
    cleaned = re.sub(r"^\d+/\d+\s*[-–—:]\s*", "", cleaned)
    
    # Strip trailing author signatures like "- Emily W." or "— Mark T."
    cleaned = re.sub(r"\s*[-–—]\s*[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s*$", "", cleaned)
    
    # Collapse multiple whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    
    return cleaned



def score_sentiment(text: str) -> tuple[float, str]:
    """Score review sentiment using local Ollama LLM.
    
    Returns score (0-10) and label (positive/neutral/negative).
    Falls back to VADER if LLM call fails - graceful degradation pattern.
    """
    prompt = f"""Analyze the sentiment of this customer review.

Review: "{text}"

Score the sentiment from 0.0 to 10.0 using ONE DECIMAL PLACE.
Use the full range - avoid round numbers like 5.0, 8.0, 9.0.

Examples of good scoring:
- "It was perfect!" → 9.4
- "Pretty good overall" → 7.2  
- "It was fine" → 5.1
- "Disappointing" → 3.3
- "Worst experience ever" → 0.6

Return JSON with:
- "score": a decimal number between 0.0 and 10.0 (one decimal place)
- "label": "positive", "neutral", or "negative"

Only return the JSON object."""

    try:
        response = ollama.generate(
            model="llama3.2",
            prompt=prompt,
            format="json",
            options={"temperature": 0.1}  # low temp for consistent scoring
        )
        
        parsed = json.loads(response["response"])
        score = float(parsed["score"])
        label = parsed["label"].lower()
        
        # validate label
        if label not in ("positive", "neutral", "negative"):
            label = "neutral"
        
        # clamp score to expected range
        score = max(0.0, min(10.0, score))
        
        return score, label
        
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        # fallback to VADER if LLM scoring fails
        print(f"⚠️  LLM scoring failed, falling back to VADER: {e}")
        compound = analyzer.polarity_scores(text)["compound"]
        score = (compound + 1) * 5
        if compound >= 0.05:
            label = "positive"
        elif compound <= -0.05:
            label = "negative"
        else:
            label = "neutral"
        return score, label


def transform_business(place_id: str) -> pa.Table | None:
    """Read Bronze data for a business, transform, and return Silver-ready Arrow table."""
    bronze_path = f"s3://{S3_BUCKET}/bronze/reviews/place_id={place_id}"
    
    print(f"📖 Reading Bronze data from {bronze_path}")
    bronze_dt = DeltaTable(bronze_path, storage_options=storage_options)
    df = bronze_dt.to_pandas()
    
    if df.empty:
        print(f"⚠️  No Bronze data for {place_id}")
        return None
    
    print(f"  Read {len(df)} raw rows")
    
    # Step 1: clean text
    df["cleaned_text"] = df["review_text"].apply(clean_review_text)
    
    # Step 2: filter out empty/invalid reviews
    initial_count = len(df)
    df = df[df["cleaned_text"].str.len() >= 10]  # at least 10 chars to be meaningful
    print(f"  Filtered out {initial_count - len(df)} empty/invalid reviews")
    
    # Step 3: deduplicate
    pre_dedup = len(df)
    df = df.drop_duplicates(subset=["cleaned_text"], keep="first")
    print(f"  Removed {pre_dedup - len(df)} duplicate reviews")
    
    if df.empty:
        print(f"⚠️  All rows filtered out for {place_id}")
        return None
    
    # Step 4: sentiment analysis with LLM
    print(f"  🤖 Analyzing sentiment via LLM for {len(df)} reviews...")
    print(f"     (this takes ~1s per review, total ~{len(df)} seconds)")

    sentiment_results = []
    for i, text in enumerate(df["cleaned_text"], 1):
        if i % 10 == 0:
            print(f"     Processed {i}/{len(df)} reviews...")
        sentiment_results.append(score_sentiment(text))

    df["sentiment_score"] = [r[0] for r in sentiment_results]
    df["sentiment_label"] = [r[1] for r in sentiment_results]
    
    # Step 5: enrichment columns
    df["word_count"] = df["cleaned_text"].str.split().str.len().astype("int32")
    df["char_count"] = df["cleaned_text"].str.len().astype("int32")
    df["processed_at"] = datetime.now(timezone.utc)
    
    # Step 6: rename and reshape to match Silver schema
    df = df.rename(columns={"review_text": "raw_review_text"})
    
    silver_df = df[[
        "ingestion_id", "place_id", "business_name", "address",
        "source", "ingested_at", "week_start",
        "raw_review_text", "cleaned_text",
        "sentiment_score", "sentiment_label",
        "word_count", "char_count", "processed_at"
    ]]
    
    # Convert to Arrow table with explicit schema enforcement
    table = pa.Table.from_pandas(silver_df, schema=silver_review_schema, preserve_index=False)
    
    print(f"✅ Transformed {table.num_rows} rows for {place_id}")
    return table


def write_to_silver(table: pa.Table):
    """Write Silver Arrow table to S3 partitioned by week_start."""
    silver_path = f"s3://{S3_BUCKET}/silver/reviews"
    
    write_deltalake(
        silver_path,
        table,
        mode="append",  # we'll switch to overwrite at the orchestration level
        storage_options=storage_options,
        partition_by=["week_start"],
        schema_mode="merge",
    )
    
    print(f"☁️  Written {table.num_rows} rows to {silver_path}")

def reset_silver():
    """Clear Silver layer for a clean rerun."""
    silver_path = f"s3://{S3_BUCKET}/silver/reviews"
    try:
        dt = DeltaTable(silver_path, storage_options=storage_options)
        dt.delete()
        print("🗑️  Silver layer cleared")
    except Exception as e:
        print(f"No existing Silver to clear: {e}")
        
def transform_all():
    """Transform all businesses from Bronze to Silver."""
    reset_silver()

    for business in MOCK_BUSINESSES:
        place_id = business["place_id"]
        business_name = business["business_name"]
        
        print(f"\n{'=' * 60}")
        print(f"Processing {business_name}")
        print('=' * 60)
        
        table = transform_business(place_id)
        if table is not None:
            write_to_silver(table)
    
    print("\n🎉 Silver transformation complete!")


if __name__ == "__main__":
    transform_all()