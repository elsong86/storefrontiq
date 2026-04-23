from fastapi import APIRouter, HTTPException, Request, Depends  
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv
from typing import Callable  
from app.utils.rate_limiter import rate_limiter
from app.utils.redis_utils import redis_client
from app.dependencies.auth import session_token_dependency

load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
router = APIRouter()

class AddressRequest(BaseModel):
    address: str

@router.post(
    "/geocode",
    # Add the session token dependency here
    dependencies=[Depends(session_token_dependency)]
)
def get_geocode(
    request: AddressRequest, 
    limiter: Callable[[Request], None] = Depends(rate_limiter(redis_client, rate=1.0, capacity=10))
):
    geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={requests.utils.quote(request.address)}&key={api_key}"

    try:
        response = requests.get(geocode_url)
        response.raise_for_status()
        data = response.json()

        if data["status"] != "OK":
            raise HTTPException(status_code=400, detail="Error geocoding address")

        location = data["results"][0]["geometry"]["location"]
        return {"latitude": location["lat"], "longitude": location["lng"]}
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))
