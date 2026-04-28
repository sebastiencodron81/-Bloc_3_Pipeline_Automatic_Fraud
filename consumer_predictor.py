"""
consumer_predictor.py
=====================
Consumer Kafka qui lit raw-transactions, predit, et republie dans
fraud-predictions.

Sequence pour chaque message :
  1. Parse le JSON
  2. Cree un DataFrame d'une ligne avec les colonnes brutes attendues
  3. Le pipeline scikit-learn applique feature engineering + preprocessing
     + prediction (le tout serialise dans model.pkl)
  4. Construit ScoredTransaction et publie dans fraud-predictions
  5. Commit l'offset Kafka uniquement si succes (idempotence)

Usage :
  python consumer_predictor.py

Variables d'environnement :
  KAFKA_BOOTSTRAP, MODEL_PATH, TOPIC_RAW, TOPIC_PREDICTIONS, GROUP_ID
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

from features import FraudFeatureBuilder  # noqa: F401  (necessaire pour unpickle)
from schemas import ScoredTransaction


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MODEL_PATH = os.getenv("MODEL_PATH", "model.pkl")
TOPIC_RAW = os.getenv("TOPIC_RAW", "raw-transactions")
TOPIC_PRED = os.getenv("TOPIC_PREDICTIONS", "fraud-predictions")
GROUP_ID = os.getenv("GROUP_ID", "fraud-consumer-group")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][predictor] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# COLONNES ATTENDUES PAR LE PIPELINE
# ----------------------------------------------------------------------

REQUIRED = [
    "trans_date_trans_time", "dob", "amt", "category", "gender",
    "city_pop", "job", "state", "lat", "long", "merch_lat", "merch_long",
]


# ----------------------------------------------------------------------
# UTIL
# ----------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def predict_one(model, payload: dict) -> tuple[int, float]:
    """Lance le pipeline sur un dict, retourne (is_fraud, score)."""
    df = pd.DataFrame([{k: payload[k] for k in REQUIRED}])
    pred = int(model.predict(df)[0])
    score = float(model.predict_proba(df)[0, 1])
    return pred, score


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main() -> None:
    log.info(f"Chargement du modele depuis {MODEL_PATH}...")
    model = joblib.load(MODEL_PATH)
    log.info("Modele charge.")

    log.info(f"Connexion Kafka {KAFKA_BOOTSTRAP} (consumer={GROUP_ID}, producer)")
    consumer = KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,                                  # commit manuel apres succes
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
    )

    n_processed = 0
    n_fraud = 0

    try:
        for msg in consumer:
            payload = msg.value
            try:
                is_fraud, score = predict_one(model, payload)
            except Exception as e:
                log.error(f"Erreur prediction sur trans_num={payload.get('trans_num')} : {e}")
                # On NE commit PAS l'offset pour pouvoir rejouer plus tard
                continue

            scored = ScoredTransaction(
                trans_num=payload["trans_num"],
                cc_num_hash=payload["cc_num_hash"],
                trans_date_trans_time=payload["trans_date_trans_time"],
                merchant=payload["merchant"],
                category=payload["category"],
                amt=payload["amt"],
                state=payload["state"],
                city=payload["city"],
                is_fraud=is_fraud,
                score=score,
                predicted_at=now_iso(),
            )

            producer.send(
                TOPIC_PRED,
                key=scored.trans_num,
                value=scored.model_dump(),
            )
            consumer.commit()                                      # commit apres publication

            n_processed += 1
            if is_fraud:
                n_fraud += 1
                log.warning(
                    f"FRAUDE detectee | trans={scored.trans_num} | "
                    f"amt={scored.amt} | score={score:.3f}"
                )
            if n_processed % 50 == 0:
                log.info(f"Stats | total={n_processed} | fraudes={n_fraud}")

    except KeyboardInterrupt:
        log.info("Arret demande.")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        log.info(f"Fermeture | total={n_processed} | fraudes={n_fraud}")


if __name__ == "__main__":
    main()
