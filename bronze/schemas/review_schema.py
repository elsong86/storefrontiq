import pyarrow as pa

review_schema = pa.schema([
    pa.field("ingestion_id", pa.string()),        # unique ID for each ingestion record
    pa.field("place_id", pa.string()),             # Google Place ID
    pa.field("business_name", pa.string()),        # business display name
    pa.field("address", pa.string()),              # formatted address
    pa.field("review_text", pa.string()),          # raw review text
    pa.field("source", pa.string()),               # e.g. "outscraper"
    pa.field("ingested_at", pa.timestamp("us")),   # when we pulled it
    pa.field("week_start", pa.date32()),           # Monday of the ingestion week
])

