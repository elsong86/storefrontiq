import uuid
import pyarrow as pa
from deltalake import write_deltalake
from dotenv import load_dotenv
from datetime import datetime, timezone, date, timedelta
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from bronze.schemas.review_schema import review_schema
from bronze.scripts.mock_data import MOCK_BUSINESSES, generate_mock_reviews

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")

def get_week_start() -> date:
    """Get the Monday of the current week."""
    today = date.today()
    return today - timedelta(days=today.weekday())

def build_arrow_table(place_id: str, business_name: str, address: str, reviews: list[dict]) -> pa.Table:
    """Convert raw reviews into a PyArrow table matching our Bronze schema."""
    now = datetime.now(timezone.utc)
    week_start = get_week_start()

    rows = {
        "ingestion_id": [str(uuid.uuid4()) for _ in reviews],
        "place_id": [place_id] * len(reviews),
        "business_name": [business_name] * len(reviews),
        "address": [address] * len(reviews),
        "review_text": [r["review_text"] for r in reviews],
        "source": ["mock"] * len(reviews),
        "ingested_at": [now] * len(reviews),
        "week_start": [week_start] * len(reviews),
    }

    return pa.table(rows, schema=review_schema)

def write_to_bronze(table: pa.Table, place_id: str):
    """Write PyArrow table to S3 as a Delta table."""
    s3_path = f"s3://{S3_BUCKET}/bronze/reviews/place_id={place_id}"

    storage_options = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "AWS_REGION": AWS_REGION,
    }

    write_deltalake(
        s3_path,
        table,
        mode="append",
        storage_options=storage_options,
        schema_mode="merge",
    )

    print(f"✅ Written {table.num_rows} reviews to {s3_path}")

def ingest(place_id: str, business_name: str, business_type: str, address: str):
    """Main ingestion function for a single business."""
    print(f"📥 Generating mock reviews for {business_name}...")
    reviews = generate_mock_reviews(
        business_name=business_name,
        business_type=business_type,
        n=30
    )

    if not reviews:
        print(f"⚠️  No reviews generated for {business_name}, skipping...")
        return

    print(f"🔨 Building Arrow table ({len(reviews)} reviews)...")
    table = build_arrow_table(place_id, business_name, address, reviews)

    print(f"☁️  Writing to Bronze layer...")
    write_to_bronze(table, place_id)

def ingest_all():
    """Ingest mock reviews for all mock businesses."""
    for business in MOCK_BUSINESSES:
        ingest(
            place_id=business["place_id"],
            business_name=business["business_name"],
            business_type=business["business_type"],
            address=business["address"]
        )
    print("🎉 All businesses ingested successfully!")

if __name__ == "__main__":
    ingest_all()