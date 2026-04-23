import os
import redis
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment variables from the .env file
load_dotenv()

# Determine environment and set Redis URL
environment = os.getenv("ENV", "development")
if environment == "development":
    # For local Dockerized Redis, use "redis://redis:6379" as default
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
else:
    # For production, use "REDIS_URL" which should default to "rediss://..."
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

# Parse the Redis URL and configure SSL if using `rediss://`
parsed_url = urlparse(redis_url)
redis_host = parsed_url.hostname
redis_port = parsed_url.port or 6379
redis_password = parsed_url.password
use_ssl = redis_url.startswith("rediss://")

# Initialize the Redis client with SSL settings for Heroku mini plans
redis_client = redis.Redis(
    host=redis_host,
    port=redis_port,
    password=redis_password,
    db=0,
    decode_responses=True,
    ssl=use_ssl,
    ssl_cert_reqs=None if use_ssl else "required"
)
