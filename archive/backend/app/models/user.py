from pydantic import BaseModel, EmailStr, UUID4

# Pydantic models for validation
class UserBase(BaseModel):
    email: EmailStr

    class Config:
        from_attributes = True  # Allows using SQLAlchemy instances with Pydantic validation

class UserCreate(UserBase):
    password: str

class User(UserBase):
    id: UUID4
