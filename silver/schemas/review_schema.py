import pyarrow as pa

silver_review_schema = pa.schema([
    # carried over from Bronze
    pa.field("ingestion_id", pa.string()),
    pa.field("place_id", pa.string()),
    pa.field("business_name", pa.string()),
    pa.field("address", pa.string()),
    pa.field("source", pa.string()),
    pa.field("ingested_at", pa.timestamp("us")),
    pa.field("week_start", pa.date32()),
    
    # original raw text preserved for traceability
    pa.field("raw_review_text", pa.string()),
    
    # new cleaned and enriched columns
    pa.field("cleaned_text", pa.string()),
    pa.field("sentiment_score", pa.float64()),       # VADER compound score scaled 0-10
    pa.field("sentiment_label", pa.string()),         # positive / neutral / negative
    pa.field("word_count", pa.int32()),
    pa.field("char_count", pa.int32()),
    pa.field("processed_at", pa.timestamp("us")),
])