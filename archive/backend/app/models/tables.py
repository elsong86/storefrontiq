from sqlalchemy import Column, String, DateTime, Enum as SQLAlchemyEnum, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.ext.declarative import declarative_base
from uuid import uuid4
from datetime import datetime, timezone
from enum import Enum

# Create the base for SQLAlchemy models
Base = declarative_base()

# Enum for provider types (email, google, etc.)
class ProviderEnum(str, Enum):
    email = "email"
    google = "google"
    apple = "apple"

# SQLAlchemy model for the User table
class UserTable(Base):
    __tablename__ = 'users'

    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    providers = Column(SQLAlchemyEnum(ProviderEnum), default=ProviderEnum.email)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_sign_in = Column(DateTime(timezone=True), nullable=True)

# SQLAlchemy model for Restaurant table
class Restaurant(Base):
    __tablename__ = 'restaurants'

    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    place_id = Column(String, unique=True, index=True)
    name = Column(String)
    address = Column(String)
    source = Column(String)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

# SQLAlchemy model for Review table
class Review(Base):
    __tablename__ = 'reviews'

    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    place_id = Column(String, ForeignKey('restaurants.place_id'), index=True)
    review_text = Column(Text)
    source = Column(String)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class AnonymousSession(Base):
    __tablename__ = 'anonymous_sessions'

    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    token = Column(String, unique=True, index=True, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    rate_limit = Column(String, default="standard")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_used = Column(DateTime(timezone=True), nullable=True)