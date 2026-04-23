from fastapi import APIRouter, HTTPException, Depends
from app.services.session_service import SessionService
import logging
from app.dependencies.auth import api_key_dependency

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/create-session", dependencies=[Depends(api_key_dependency)])
async def create_session(
    session_service: SessionService = Depends()
):
    logger.info("X-API-Key validated (by dependency). Proceeding to create session...")
    
    try:
        # Create a new session
        session_data = await session_service.create_anonymous_session()
        return session_data
    except Exception as e:
        logger.error(f"Error creating session: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create session")