"""Pydantic schemas partages par le producer et les consumers."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RawTransaction(BaseModel):
    """Transaction brute telle que recue de l'API source."""

    model_config = ConfigDict(coerce_numbers_to_str=True, extra="ignore")

    cc_num: str = Field(min_length=10, max_length=20)
    merchant: str
    category: str
    amt: float = Field(ge=0, le=50_000)
    first: str
    last: str
    gender: Literal["M", "F"]
    street: str
    city: str
    state: str = Field(min_length=2, max_length=2)
    zip: int = Field(ge=0, le=99999)
    lat: float = Field(ge=-90, le=90)
    long: float = Field(ge=-180, le=180)
    city_pop: int = Field(ge=0)
    job: str
    dob: str
    trans_num: str
    merch_lat: float = Field(ge=-90, le=90)
    merch_long: float = Field(ge=-180, le=180)
    current_time: int = Field(ge=0)
    is_fraud: Optional[int] = Field(default=None, ge=0, le=1)

    @field_validator("dob")
    @classmethod
    def _parse_dob(cls, v: str) -> str:
        datetime.fromisoformat(v)
        return v


class ScoredTransaction(BaseModel):
    """Transaction enrichie de la prediction du modele."""

    trans_num: str
    cc_num_hash: str
    trans_date_trans_time: str
    merchant: str
    category: str
    amt: float
    state: str
    city: str
    is_fraud: int = Field(ge=0, le=1)
    score: float = Field(ge=0.0, le=1.0)
    ground_truth: Optional[int] = Field(default=None, ge=0, le=1)
    predicted_at: str


class DeadLetter(BaseModel):
    """Message rejete par la validation."""

    rejected_at: str
    error: str
    raw_payload: dict
