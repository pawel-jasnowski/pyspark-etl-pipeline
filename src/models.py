# src/models.py
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, ValidationError


class RawTransaction(BaseModel):
    """
    data validation for transaction file.
    """

    transaction_id: UUID
    customer_id: UUID
    # Używamy float, bo inferSchema w Sparku wczyta to jako Double.
    # Walidujemy tylko, czy jest to liczba > 0.
    # Precyzyjną konwersję na Decimal zrobimy później w Sparku, na poprawnych danych.
    amount: float = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    country_code: str = Field(min_length=2, max_length=2)
    # Pydantic automatycznie spróbuje sparsować string z datą do obiektu datetime.
    timestamp: datetime

    class Config:
        # Pozwala Pydantic na pracę z obiektami, które nie są słownikami,
        # np. z wierszami Spark DataFrame.
        from_attributes = True
