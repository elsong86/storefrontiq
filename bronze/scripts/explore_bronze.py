from deltalake import DeltaTable
import os
from dotenv import load_dotenv

load_dotenv()

storage_options = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_REGION": os.getenv("AWS_REGION"),
}

bucket = os.getenv("S3_BUCKET")

# explore one business in depth
place_id = "ChIJN1t_tDeuEmsRUsoyG83frY4"
s3_path = f"s3://{bucket}/bronze/reviews/place_id={place_id}"

dt = DeltaTable(s3_path, storage_options=storage_options)
df = dt.to_pandas()

print(f"Total reviews: {len(df)}")
print("=" * 70)
print("\n📋 ALL REVIEWS (Mario's Pizzeria):\n")

for i, row in df.iterrows():
    print(f"[{i+1}] {row['review_text']}")
    print()

print("=" * 70)
print("📊 DATA QUALITY CHECKS\n")

# duplicate detection
duplicates = df[df.duplicated(subset=["review_text"], keep=False)]
print(f"Exact duplicate reviews: {len(duplicates)}")

# length analysis
df["char_length"] = df["review_text"].str.len()
df["word_count"] = df["review_text"].str.split().str.len()
print(f"Review length range: {df['char_length'].min()} - {df['char_length'].max()} chars")
print(f"Word count range: {df['word_count'].min()} - {df['word_count'].max()} words")

# check for non-review noise
print(f"\nReviews with 'star' rating embedded: {df['review_text'].str.contains(r'\d/\d', regex=True).sum()}")
print(f"Reviews with author signatures (like '- Emily'): {df['review_text'].str.contains(r'-\s*[A-Z][a-z]+', regex=True).sum()}")
print(f"Reviews mentioning '(positive)' or similar labels: {df['review_text'].str.contains(r'\((positive|negative|neutral)\)', case=False, regex=True).sum()}")
print(f"Empty/whitespace-only reviews: {(df['review_text'].str.strip() == '').sum()}")