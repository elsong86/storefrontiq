from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel, Field
from typing import List, Optional, Callable
import requests
import os
from dotenv import load_dotenv
from app.utils.rate_limiter import rate_limiter
from app.utils.redis_utils import redis_client
import json
import logging

# Load environment variables from the .env file
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

# Get Google API key from environment variables
api_key = os.getenv("GOOGLE_API_KEY")

router = APIRouter()

class PhotoRequest(BaseModel):
    photo_name: str = Field(..., description="The resource name of the photo")
    max_height: Optional[int] = Field(None, description="Maximum height in pixels (1-4800)", ge=1, le=4800)
    max_width: Optional[int] = Field(None, description="Maximum width in pixels (1-4800)", ge=1, le=4800)

class Photo(BaseModel):
    url: str

@router.post("/photos", response_model=Photo)
async def get_photo_url(
    request: PhotoRequest,
    limiter: Callable[[Request], None] = Depends(rate_limiter(redis_client, rate=1.0, capacity=10))
):
    """
    Get the URL for a Google Places photo based on its resource name.
    
    This endpoint creates a properly formatted URL to fetch a photo directly from Google Places API.
    """
    # Check if at least one dimension is provided
    if request.max_height is None and request.max_width is None:
        raise HTTPException(status_code=400, detail="Either max_height or max_width (or both) must be provided")
    
    # Construct the URL parameters
    params = []
    if request.max_height is not None:
        params.append(f"maxHeightPx={request.max_height}")
    if request.max_width is not None:
        params.append(f"maxWidthPx={request.max_width}")
    params.append(f"key={api_key}")
    
    # Construct the final URL
    # The format is: https://places.googleapis.com/v1/NAME/media?key=API_KEY&PARAMETERS
    photo_url = f"https://places.googleapis.com/v1/{request.photo_name}/media?{('&').join(params)}"
    
    # Cache the request with a unique key
    cache_key = f"photo:{request.photo_name}:{request.max_height}:{request.max_width}"
    cached_url = redis_client.get(cache_key)
    
    if cached_url:
        logger.info(f"Cache hit for photo: {cache_key}")
        return {"url": cached_url}
    
    # Store the URL in cache for future requests (cache for 24 hours)
    redis_client.setex(cache_key, 86400, photo_url)
    
    return {"url": photo_url}