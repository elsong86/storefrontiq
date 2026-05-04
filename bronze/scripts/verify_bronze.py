import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

bucket = os.getenv("S3_BUCKET")

print(f"Listing contents of s3://{bucket}/bronze/")
print("-" * 60)

response = s3.list_objects_v2(Bucket=bucket, Prefix="bronze/")

if "Contents" not in response:
    print("❌ No files found")
else:
    for obj in response["Contents"]:
        size_kb = obj["Size"] / 1024
        print(f"📄 {obj['Key']} ({size_kb:.2f} KB)")
    
    print(f"\n✅ Total objects: {len(response['Contents'])}")