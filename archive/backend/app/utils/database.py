from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv
import os
import logging
from typing import AsyncGenerator

# Load environment variables from the .env file
load_dotenv()

# Fetch DATABASE_URL from environment
DATABASE_URL: str = os.getenv("DATABASE_URL")
 

if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)


# Create async engine
engine = create_async_engine(
    DATABASE_URL,
    echo=True,  # Enable SQL query logging (useful for debugging)
    pool_size=5,  # Set pool size
    max_overflow=10  # Allow additional overflow connections
)

# Create a sessionmaker factory for AsyncSession
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Dependency that will be used in FastAPI routes to provide a session
async def get_database_client() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

# Optional: Test database connection after creating engine
async def test_connection():
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            logging.info("Database connection test passed.")
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
