"""
features.py
===========
Feature engineering partage entre l'entrainement et l'inference.

Ce fichier est CRUCIAL pour le no-train-serve-skew : la meme classe est
importee par le notebook d'entrainement ET par consumer_predictor.py.
Le pipeline scikit-learn pickle cette classe par reference, donc les
deux process doivent pouvoir l'importer du meme module.

Si ce fichier change, il faut REENTRAINER le modele.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class FraudFeatureBuilder(BaseEstimator, TransformerMixin):
    """
    Construit les features derivees a partir des champs bruts d'une transaction.

    Features ajoutees :
      - hour            : heure de la transaction (0-23)
      - day_of_week     : jour de la semaine (0=lundi)
      - is_weekend      : 1 si samedi/dimanche
      - is_night        : 1 si entre 22h et 5h
      - age             : age du client en annees
      - distance_km     : distance haversine client/marchand
      - amt_log         : log1p(amt) pour normaliser la queue
    """

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()

        # Features temporelles
        ts = pd.to_datetime(X["trans_date_trans_time"])
        X["hour"] = ts.dt.hour
        X["day_of_week"] = ts.dt.dayofweek
        X["is_weekend"] = (X["day_of_week"] >= 5).astype(int)
        X["is_night"] = ((X["hour"] >= 22) | (X["hour"] <= 5)).astype(int)

        # Age
        dob = pd.to_datetime(X["dob"])
        X["age"] = ((ts - dob).dt.days / 365.25).astype(int)

        # Distance haversine
        X["distance_km"] = self._haversine(
            X["lat"].values, X["long"].values,
            X["merch_lat"].values, X["merch_long"].values
        )

        # Log du montant
        X["amt_log"] = np.log1p(X["amt"])

        return X

    @staticmethod
    def _haversine(lat1, lon1, lat2, lon2):
        """Distance en kilometres entre deux points GPS."""
        R = 6371.0
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        return 2 * R * np.arcsin(np.sqrt(a))


# Listes utilisees par le ColumnTransformer (importables ici aussi)
NUMERIC_FEATURES = [
    "amt_log", "amt", "age", "distance_km", "city_pop",
    "hour", "day_of_week", "is_weekend", "is_night",
]
CATEGORICAL_FEATURES = ["category", "gender", "state", "job"]
RAW_INPUT_COLS = [
    "trans_date_trans_time", "dob",
    "amt", "category", "gender", "city_pop", "job", "state",
    "lat", "long", "merch_lat", "merch_long",
]
