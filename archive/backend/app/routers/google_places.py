from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
import requests 
import os
from dotenv import load_dotenv
from app.utils.redis_utils import redis_client
from app.dependencies.auth import session_token_dependency
import json

load_dotenv()

api_key = os.getenv("GOOGLE_API_KEY")

router = APIRouter()

class Location(BaseModel):
    latitude: float
    longitude: float

class PlacesRequest(BaseModel):
    location: Location
    radius: float = 1000.0
    max_results: int = 20
    text_query: str = "tacos"

@router.post(
    "/places",
    # Add the session token dependency here
    dependencies=[Depends(session_token_dependency)]
)
def get_places(request: PlacesRequest): 
    cache_key = f"places:{json.dumps(request.model_dump(), sort_keys=True)}"
    cached_places = redis_client.get(cache_key)
    if cached_places: 
        return json.loads(cached_places)

    url = "https://places.googleapis.com/v1/places:searchText"
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.id,places.displayName.text,places.formattedAddress,places.location,places.types,places.userRatingCount,places.rating,places.photos"
    }

    data = {
        "textQuery": request.text_query,
        "locationBias": {
            "circle": {
                "center": {
                    "latitude": request.location.latitude,
                    "longitude": request.location.longitude
                },
                "radius": request.radius
            }
        },
        "maxResultCount": request.max_results
    }

    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        redis_client.setex(cache_key, 172800, json.dumps(response.json()))
        return response.json()
    except requests.RequestException as e:
        print(f"Error response: {e.response.text if e.response else 'No response'}")
        raise HTTPException(status_code=500, detail=f"Error calling Google Places API: {str(e)}")