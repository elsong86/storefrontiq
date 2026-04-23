from fastapi import Header, HTTPException, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
import secrets
import os

# Import your SessionService and DB client function
from app.services.session_service import SessionService
from app.utils.database import get_database_client # Adjust import path

# Load the static key ONCE (or ensure it's loaded via dotenv elsewhere)
CLIENT_API_KEY = os.getenv("CLIENT_API_KEY")
if not CLIENT_API_KEY:
     # This should ideally be caught at startup, but good to check
     raise ValueError("FATAL: CLIENT_API_KEY environment variable not set.")

# --- Dependency 1: Checks the static X-API-Key ---
# Used ONLY for the /create-session endpoint
async def api_key_dependency(x_api_key: str | None = Header(None)): # Use Header to extract
    """
    Dependency function to validate the static X-API-Key header.
    Raises HTTPException 403 if invalid or missing.
    """
    if not x_api_key or not secrets.compare_digest(x_api_key, CLIENT_API_KEY):
        raise HTTPException(
            status_code=403,
            detail="Invalid or missing API key"
        )
    # No need to return anything if just performing a check

# --- Dependency 2: Checks the dynamic X-Session-Token ---
# Used for all other protected endpoints (/places, /geocode, /reviews, /photos)
async def session_token_dependency(
    x_session_token: str | None = Header(None), # Get token from header
    db: AsyncSession = Depends(get_database_client) # Get DB session
):
    """
    Dependency function to validate the X-Session-Token header.
    Uses SessionService to check against the database.
    Raises HTTPException 401 if invalid, expired, or missing.
    """
    if not x_session_token:
        raise HTTPException(
            status_code=401,
            detail="Missing session token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create an instance of SessionService to use its validation logic
    session_service = SessionService(db=db)
    is_valid = await session_service.validate_session_token(x_session_token)

    if not is_valid:
         raise HTTPException(
             status_code=401,
             detail="Invalid or expired session token",
             headers={"WWW-Authenticate": "Bearer"},
         )
    # No need to return anything if just performing a check
    # Optionally: could query session data here and attach to request.state