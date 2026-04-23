from fastapi import APIRouter, HTTPException, Request, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.tables import UserTable
from app.utils.database import get_database_client
from app.utils.jwt_utils import decode_jwt  # JWT decoding utility we'll define below
from sqlalchemy.future import select
import logging

router = APIRouter()

@router.get("/profile")
async def get_profile(request: Request, db: AsyncSession = Depends(get_database_client)):
    # Extract the JWT token from the 'access_token' cookie
    jwt_token = request.cookies.get("access_token")
    
    if not jwt_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # Decode the JWT token to get the user ID
        payload = decode_jwt(jwt_token)
        user_id = payload.get("sub")
        
        if not user_id:
            raise HTTPException(status_code=400, detail="Invalid token payload")

        # Fetch the user from the database using the user ID from the JWT token
        result = await db.execute(select(UserTable).where(UserTable.id == user_id))
        user = result.scalar_one_or_none()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Return the user's profile information
        return {"email": user.email}
    
    except Exception as e:
        logging.error(f"Failed to retrieve profile: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to retrieve profile: {str(e)}")
