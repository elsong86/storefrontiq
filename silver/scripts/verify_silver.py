from deltalake import DeltaTable
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

storage_options = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_REGION": os.getenv("AWS_REGION"),
}

bucket = os.getenv("S3_BUCKET")
silver_path = f"s3://{bucket}/silver/reviews"

dt = DeltaTable(silver_path, storage_options=storage_options)
df = dt.to_pandas()

print(f"Silver table: {silver_path}")
print(f"Version: {dt.version()}")
print(f"Total rows: {len(df)}")
print("=" * 70)

# show the cleaning worked
print("\n🧼 CLEANING VERIFICATION (Mario's Pizzeria):\n")
mario = df[df["business_name"] == "Mario's Pizzeria"].head(5)

for _, row in mario.iterrows():
    print(f"RAW:     {row['raw_review_text'][:80]}...")
    print(f"CLEANED: {row['cleaned_text'][:80]}...")
    print(f"SCORE:   {row['sentiment_score']:.2f}/10  ({row['sentiment_label']})")
    print(f"WORDS:   {row['word_count']}")
    print("-" * 70)

# overall stats
print("\n📊 SENTIMENT DISTRIBUTION BY BUSINESS\n")
summary = df.groupby("business_name").agg(
    review_count=("ingestion_id", "count"),
    avg_sentiment=("sentiment_score", "mean"),
    avg_words=("word_count", "mean"),
).round(2)
print(summary.to_string())

print("\n📊 SENTIMENT LABEL BREAKDOWN\n")
label_dist = df.groupby(["business_name", "sentiment_label"]).size().unstack(fill_value=0)
print(label_dist.to_string())
print("\n📊 SENTIMENT SCORE DISTRIBUTION (all reviews)\n")
print(df["sentiment_score"].value_counts().sort_index().to_string())