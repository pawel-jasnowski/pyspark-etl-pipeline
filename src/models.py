# src/models.py
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, ValidationError


class RawTransaction(BaseModel):
    """
    data validation for transaction file
    Usually Pydantic works with Python DICT types BUT our ROW from SPARK is not a classic dict type,
    so we need class Config here and from_attributes = True to read those object properly
    """

    transaction_id: UUID
    customer_id: UUID
    amount: float = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    country_code: str = Field(min_length=2, max_length=2)
    timestamp: datetime
    source_file: str

    class Config:
        from_attributes = True
