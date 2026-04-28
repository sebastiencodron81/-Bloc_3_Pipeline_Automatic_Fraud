"""
schemas.py
==========
Schemas Pydantic partages entre tous les composants du pipeline.

Centraliser les schemas garantit que producer, consumers, et tests
parlent tous le meme langage et que toute incompatibilite saute aux
yeux a la moindre modification.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ----------------------------------------------------------------------
# SCHEMA D'UNE TRANSACTION BRUTE (telle que fournie par l'API)
# ----------------------------------------------------------------------

class RawTransaction(BaseModel):
    """
    Schema d'une transaction telle que produite par fake_api.py
    (et par la vraie API si elle revient un jour).

    Note : coerce_numbers_to_str=True permet a Pydantic de caster
    automatiquement les nombres en string (ex: cc_num que pandas
    serialise en int) sans rejeter le message.
    """

    model_config = ConfigDict(coerce_numbers_to_str=True)

    trans_date_trans_time: str
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
    unix_time: int
    merch_lat: float = Field(ge=-90, le=90)
    merch_long: float = Field(ge=-180, le=180)

    @field_validator("trans_date_trans_time", "dob")
    @classmethod
    def parse_date(cls, v: str) -> str:
        # Verifie que le format est parsable, mais on garde la string
        datetime.fromisoformat(v.replace(" ", "T"))
        return v


# ----------------------------------------------------------------------
# SCHEMA D'UNE PREDICTION ENRICHIE (publiee dans fraud-predictions)
# ----------------------------------------------------------------------

class ScoredTransaction(BaseModel):
    """
    Transaction apres passage par le modele : meme contenu mais
    avec is_fraud + score, et cc_num pseudonymise.
    """
    trans_num: str
    cc_num_hash: str             # SHA-256 de cc_num (RGPD)
    trans_date_trans_time: str
    merchant: str
    category: str
    amt: float
    state: str
    city: str
    is_fraud: int = Field(ge=0, le=1)
    score: float = Field(ge=0.0, le=1.0)
    predicted_at: str            # ISO timestamp UTC


# ----------------------------------------------------------------------
# SCHEMA D'UN MESSAGE EN DLQ
# ----------------------------------------------------------------------

class DeadLetter(BaseModel):
    """Message rejete avec le motif et le payload original."""
    rejected_at: str
    error: str
    raw_payload: dict
