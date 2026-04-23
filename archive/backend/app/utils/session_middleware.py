from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from app.services.session_service import SessionService
from app.utils.database import get_database_client
import logging

logger = logging.getLogger(__name__)

# List of paths that don't require session authentication
PUBLIC_PATHS = [
    "/docs", 
    "/redoc", 
    "/openapi.json",
    "/create-session"  # This endpoint must be public to create a session
]

async def verify_session_token(request: Request, call_next):
    # Skip authentication for public paths
    if any(request.url.path.endswith(path) for path in PUBLIC_PATHS):
        return await call_next(request)
    
    # Get session token from header
    session_token = request.headers.get("X-Session-Token")
    
    if not session_token:
        logger.warning("Missing session token")
        return JSONResponse(
            status_code=401,
            content={"detail": "Missing session token"}
        )
    
    # Initialize database and session service
    async for db in get_database_client():
        session_service = SessionService(db)
        
        # Validate the token
        is_valid = await session_service.validate_session_token(session_token)
        
        if not is_valid:
            logger.warning("Invalid or expired session token")
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired session token"}
            )
        
        # Token is valid, proceed
        break
    
    return await call_next(request)