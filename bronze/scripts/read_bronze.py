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
place_id = "ChIJN1t_tDeuEmsRUsoyG83frY4"  # Mario's Pizzeria
s3_path = f"s3://{bucket}/bronze/reviews/place_id={place_id}"

dt = DeltaTable(s3_path, storage_options=storage_options)

print(f"Delta table: {s3_path}")
print(f"Current version: {dt.version()}")
print(f"Schema: {dt.schema()}")
print("-" * 60)

# read latest version as Pandas DataFrame
df = dt.to_pandas()
print(f"\nLatest version row count: {len(df)}")
print(f"\nFirst 3 reviews:")
for i, row in df.head(3).iterrows():
    print(f"\n[{row['business_name']}] {row['review_text'][:100]}...")

# demonstrate time travel
print("\n" + "=" * 60)
print("⏰ TIME TRAVEL DEMO")
print("=" * 60)
for v in range(dt.version() + 1):
    dt_v = DeltaTable(s3_path, version=v, storage_options=storage_options)
    df_v = dt_v.to_pandas()
    print(f"Version {v}: {len(df_v)} rows")