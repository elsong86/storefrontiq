from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from app.utils.database import engine, test_connection
from app.models.tables import Base
from app.routers import google_places, google_geocode, outscraper_reviews, auth, profile, place_photos, sessions
from app.utils.api_key_middleware import verify_api_key
from app.utils.session_middleware import verify_session_token
import os

# Global session factory
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic: Create the database tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Test the connection (optional)
    await test_connection()

    # Yield control to the application
    yield

    # Shutdown logic: Dispose of the database engine
    await engine.dispose()

# Initialize the FastAPI app with the lifespan context manager
app = FastAPI(lifespan=lifespan)

# Include all routers
app.include_router(sessions.router)  # Add new sessions router
app.include_router(google_places.router)
app.include_router(google_geocode.router)
app.include_router(outscraper_reviews.router)
app.include_router(auth.router)
app.include_router(profile.router)
app.include_router(place_photos.router)

# Dependency for getting a database session
async def get_db():
    async with SessionLocal() as session:
        yield session

if __name__ == "__main__" and os.getenv("APP_ENV") == "development":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)