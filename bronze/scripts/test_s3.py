import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def test_s3_connection():
    print("Testing S3 connection...")
    print("-" * 50)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    bucket = os.getenv("S3_BUCKET")

    try:
        s3.head_bucket(Bucket=bucket)
        print(f"✅ Successfully connected to bucket: {bucket}")
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    test_s3_connection()