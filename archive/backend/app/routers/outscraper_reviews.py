from fastapi import APIRouter, HTTPException, Query, Depends, Request
from outscraper import ApiClient
import logging
import json
import os
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError
from ..utils.rate_limiter import rate_limiter
from ..utils.redis_utils import redis_client
from ..services.database_service import DatabaseService
from ..utils.sentiment_analysis import analyze_sentiments
from lingua import Language, LanguageDetectorBuilder
from pydantic import BaseModel, Field
from typing import Callable
from app.models.tables import Restaurant, Review
from app.utils.database import get_database_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Outscraper API client
api_key = os.getenv("OUTSCRAPER_API_KEY")  # Make sure this is set up in your environment variables
client = ApiClient(api_key)

# Initialize Lingua language detector with English enabled
detector = LanguageDetectorBuilder.from_languages(Language.ENGLISH, Language.SPANISH).build()

# Define the is_english function
def is_english(text):
    try:
        # Detect language using Lingua
        detected_language = detector.detect_language_of(text)
        return detected_language == Language.ENGLISH
    except Exception as e:
        logger.error(f"Error detecting language: {e}")
        return False

router = APIRouter()

def check_cache(place_id: str):
    cache_key = f"reviews:{place_id}"
    cached_reviews = redis_client.get(cache_key)
    if cached_reviews:
        logger.info(f"Cache hit for place_id: {place_id}")
        return json.loads(cached_reviews)
    return None

async def fetch_reviews_from_api(place_id: str):
    try:
        results = client.google_maps_reviews(
            place_id,
            reviews_limit=30,
            language='en',
            fields="reviews_data.review_text",
            sort="newest"
        )
        if results and isinstance(results, list) and len(results) > 0:
            reviews = results[0].get('reviews_data', [])
            
            # Filter out non-English reviews using is_english
            english_reviews = [review for review in reviews if is_english(review['review_text'])]

            non_empty_reviews = [review for review in english_reviews if review.get('review_text') and review['review_text'].strip()]

            # Cache the fetched reviews
            cache_key = f"reviews:{place_id}"
            redis_client.setex(cache_key, 86400, json.dumps(non_empty_reviews))  # Cache for 1 hour
            logger.info(f"Cached reviews for place_id: {place_id}")

            return non_empty_reviews
        return []
    except Exception as e:
        logger.error(f"Error fetching reviews from API: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def store_restaurant(place_id: str, name: str, address: str, db: AsyncSession):
    try:
        async with db.begin():  # Ensure transaction consistency
            result = await db.execute(select(Restaurant).where(Restaurant.place_id == place_id))
            existing_restaurant = result.scalar_one_or_none()

            if not existing_restaurant:
                new_restaurant = Restaurant(
                    place_id=place_id,
                    name=name,
                    address=address,
                    source="outscraper_api",
                    created_at=datetime.now(timezone.utc)
                )
                db.add(new_restaurant)
                await db.commit()
                logging.info(f"Restaurant stored successfully: {name}")
            else:
                logging.info(f"Restaurant already exists: {name}")
    except SQLAlchemyError as e:
        await db.rollback()  # Rollback on SQLAlchemy errors
        logger.error(f"Failed to store restaurant: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def get_stored_reviews(place_id: str, db: AsyncSession):
    freshness_limit = timedelta(weeks=1)
    now = datetime.now(timezone.utc)

    try:
        async with db.begin():  # Use context to handle transaction
            result = await db.execute(
                select(Review).where(Review.place_id == place_id).order_by(Review.created_at.desc()).limit(30)
            )
            reviews = result.scalars().all()

            if reviews:
                latest_review = max(reviews, key=lambda r: r.created_at)
                review_age = now - latest_review.created_at

                if review_age < freshness_limit:
                    logger.info(f"Found recent reviews in the database for place_id: {place_id}")

                    # Convert ORM instances to dictionaries before caching and returning
                    review_dicts = [
                        {
                            "id": str(review.id),
                            "place_id": review.place_id,
                            "review_text": review.review_text,
                            "source": review.source,
                            "created_at": review.created_at.isoformat()
                        }
                        for review in reviews
                    ]

                    # Cache the fetched reviews
                    cache_key = f"reviews:{place_id}"
                    redis_client.setex(cache_key, 86400, json.dumps(review_dicts))
                    logger.info(f"Cached reviews from database for place_id: {place_id}")

                    return review_dicts
        logger.info(f"No recent reviews found in the database for place_id: {place_id}")
        return None
    except SQLAlchemyError as e:
        await db.rollback()  # Rollback on error
        logger.error(f"Error fetching reviews from database: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

class ReviewQueryParams(BaseModel):
    place_id: str = Field(..., alias="place_id", pattern=r"^[A-Za-z0-9_\-]+$")
    name: str = Field(..., alias="displayName", min_length=1, max_length=100)
    address: str = Field(..., alias="formattedAddress", min_length=5, max_length=255)

@router.get("/reviews")
async def get_reviews(
    query_params: ReviewQueryParams = Depends(),
    db: AsyncSession = Depends(get_database_client),
    limiter: Callable[[Request], None] = Depends(rate_limiter(redis_client, rate=1.0, capacity=10))
):
    place_id = query_params.place_id
    name = query_params.name
    address = query_params.address
    
    logger.info(f"Received place_id: {place_id}")
    logger.info(f"Received displayName: {name}")
    logger.info(f"Received formattedAddress: {address}")
    
    try:
        # Step 1: Check the Redis cache first
        cached_reviews = check_cache(place_id)
        if cached_reviews:
            average_sentiment = analyze_sentiments(cached_reviews)
            return {"average_sentiment": average_sentiment, "reviews": cached_reviews, "source": "cache"}

        # Step 2: If not found in Redis, check the database
        stored_reviews = await get_stored_reviews(place_id, db)
        if stored_reviews:
            average_sentiment = analyze_sentiments(stored_reviews)
            return {"average_sentiment": average_sentiment, "reviews": stored_reviews, "source": "database"}

        # Step 3: If not found in Redis or database, fetch from Outscraper API
        reviews = await fetch_reviews_from_api(place_id)

        # Store the restaurant details before storing reviews
        await store_restaurant(place_id, name, address, db)

        # Store fetched reviews in the database
        async with db.begin():  # Transaction context for storing reviews
            for review in reviews:
                new_review = Review(
                    place_id=place_id,
                    review_text=review['review_text'],
                    source="outscraper_api",
                    created_at=datetime.now(timezone.utc)
                )
                db.add(new_review)
            await db.commit()  # Explicit commit

        average_sentiment = analyze_sentiments(reviews)
        return {"average_sentiment": average_sentiment, "reviews": reviews, "source": "api"}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))