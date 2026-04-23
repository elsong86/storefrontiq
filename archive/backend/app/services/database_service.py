import logging
import jwt  # For JWT creation
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError
from app.models.tables import UserTable, Review  # Assuming you have these models
from app.utils.database import get_database_client
import bcrypt  # Using bcrypt directly for password hashing
from fastapi import Depends
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Fetch the required values from .env
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")  # Default to HS256 if not provided
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 15))  # Default to 15 minutes

logging.basicConfig(level=logging.INFO)


class DatabaseService:
    def __init__(self, db: AsyncSession = Depends(get_database_client)):
        self.db = db

    # Password hashing utility using bcrypt directly
    def hash_password(self, password: str) -> str:
        pwd_bytes = password.encode('utf-8')  # Convert password to bytes
        salt = bcrypt.gensalt()  # Generate a salt
        hashed_password = bcrypt.hashpw(pwd_bytes, salt)  # Hash the password
        return hashed_password

    # Password verification utility using bcrypt directly
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        password_byte_enc = plain_password.encode('utf-8')  # Convert password to bytes
        return bcrypt.checkpw(password_byte_enc, hashed_password.encode('utf-8'))  # Verify the password

    # Validate email and password format
    def validate_credentials(self, email, password):
        if '@' not in email or len(password) < 8:
            logging.error("Invalid credentials provided.")
            return False
        return True

    # Sign up method (registering a new user)
    async def sign_up(self, email, password):
        if not self.validate_credentials(email, password):
            return {"error": "Invalid email or password format"}

        hashed_password = self.hash_password(password).decode('utf-8')  # Decode the hashed password to store as string

        new_user = UserTable(email=email, hashed_password=hashed_password)
        
        try:
            self.db.add(new_user)
            await self.db.commit()
            logging.info(f"User signed up successfully: {email}")
            return {"message": "User created successfully"}
        except SQLAlchemyError as e:
            logging.error(f"Signup failed for {email}: {str(e)}")
            return {"error": "Error during signup"}

    # Sign in method (user login)
    async def sign_in(self, email, password):
        if not self.validate_credentials(email, password):
            return {"error": "Invalid email or password format"}

        try:
            # Fetch user by email
            result = await self.db.execute(select(UserTable).where(UserTable.email == email))
            user = result.scalar_one_or_none()

            if user and self.verify_password(password, user.hashed_password):
                logging.info(f"User signed in successfully: {email}")
                
                # Update last_sign_in timestamp
                user.last_sign_in = datetime.now(timezone.utc)
                await self.db.commit()  # Commit the changes to the database
                
                # Generate a JWT token for the user
                access_token = self.create_access_token(data={"sub": str(user.id)})
                
                return {
                    "message": "Login successful",
                    "access_token": access_token,
                    "user_id": user.id
                }
            else:
                logging.error(f"Sign in failed for {email}: Invalid credentials")
                return {"error": "Invalid email or password"}

        except SQLAlchemyError as e:
            logging.error(f"Sign in failed for {email}: {str(e)}")
            return {"error": "Error during sign in"}

    # Create a JWT token
    def create_access_token(self, data: dict, expires_delta: timedelta = None):
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + (expires_delta if expires_delta else timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt

    # Store a review in the PostgreSQL database
    async def store_review(self, place_id: str, review_text: str):
        new_review = Review(place_id=place_id, review_text=review_text, source="outscraper_api")

        try:
            self.db.add(new_review)
            await self.db.commit()
            logging.info(f"Review stored successfully: {review_text}")
            return {"message": "Review stored successfully"}
        except SQLAlchemyError as e:
            logging.error(f"Failed to store review: {str(e)}")
            return {"error": "Error storing review"}

    # Fetch user profile
    async def fetch_user(self, user_id: str):
        try:
            logging.info(f"Fetching user with ID: {user_id}")

            # Fetch the user by ID from the database
            result = await self.db.execute(select(UserTable).where(UserTable.id == user_id))
            user = result.scalar_one_or_none()

            if user:
                logging.info(f"User fetched successfully: {user_id}")
                return {"user": {"id": user.id, "email": user.email, "last_sign_in": user.last_sign_in}}
            else:
                logging.error("User not found")
                return {"error": "User not found"}

        except SQLAlchemyError as e:
            logging.error(f"Failed to fetch user: {str(e)}")
            return {"error": "Error fetching user"}
