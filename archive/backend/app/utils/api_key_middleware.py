from fastapi import Request
from fastapi.responses import JSONResponse
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_API_KEY = os.getenv("CLIENT_API_KEY")
if not CLIENT_API_KEY:
    raise ValueError("CLIENT_API_KEY must be set in the environment variables")

async def verify_api_key(request: Request, call_next):
    # Exclude authentication endpoints from API key requirement
    public_paths = ["/docs", "/redoc", "/openapi.json"]
    if request.url.path in public_paths:
        return await call_next(request)
        
    # Get API key from header
    api_key = request.headers.get("X-Session-Token")
    
    if not api_key or api_key != CLIENT_API_KEY:
        return JSONResponse(
            status_code=403,
            content={"detail": "Invalid or missing API key"}
        )
    
    return await call_next(request)