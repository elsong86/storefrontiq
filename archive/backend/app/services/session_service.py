import secrets
import string
import logging
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError
from app.models.tables import AnonymousSession
from app.utils.database import get_database_client
from fastapi import Depends
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# App secret for validating app requests
APP_SECRET = os.getenv("CLIENT_API_KEY")

class SessionService:
    def __init__(self, db: AsyncSession = Depends(get_database_client)):
        self.db = db
    
    # Validate the app secret from the request
    def validate_app_secret(self, app_secret: str) -> bool:
        # In production, use a secure comparison to prevent timing attacks
        return secrets.compare_digest(app_secret, APP_SECRET)
    
    # Generate a secure random token
    def generate_secure_token(self, length: int = 32) -> str:
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(length))
    
    # Create a new anonymous session
    async def create_anonymous_session(self, duration_days: int = 7, rate_limit: str = "standard") -> dict:
        try:
            # Generate a secure token
            token = self.generate_secure_token()
            
            # Calculate expiry
            expiry = datetime.now(timezone.utc) + timedelta(days=duration_days)
            
            # Create session record
            new_session = AnonymousSession(
                token=token,
                expires_at=expiry,
                rate_limit=rate_limit,
                created_at=datetime.now(timezone.utc)
            )
            
            # Save to database
            self.db.add(new_session)
            await self.db.commit()
            
            logger.info(f"Created new anonymous session, expires: {expiry}")
            
            return {
                "token": token,
                "expiresAt": expiry
            }
        except SQLAlchemyError as e:
            logger.error(f"Failed to create anonymous session: {str(e)}")
            await self.db.rollback()
            raise
    
    # Validate a session token
    async def validate_session_token(self, token: str) -> bool:
        try:
            # Find the session
            result = await self.db.execute(
                select(AnonymousSession).where(AnonymousSession.token == token)
            )
            session = result.scalar_one_or_none()
            
            # Check if session exists and is not expired
            if not session:
                logger.warning(f"Session token not found")
                return False
            
            if session.expires_at < datetime.now(timezone.utc):
                logger.warning(f"Session token expired at {session.expires_at}")
                return False
            
            # Update last used timestamp
            session.last_used = datetime.now(timezone.utc)
            await self.db.commit()
            
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error validating session token: {str(e)}")
            return False