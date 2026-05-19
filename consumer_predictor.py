"""Consumer Kafka : lit raw-transactions, score le modele, publie fraud-predictions."""

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


REQUIRED = [
    "trans_date_trans_time",
    "dob",
    "amt",
    "category",
    "gender",
    "city_pop",
    "job",
    "state",
    "lat",
    "long",
    "merch_lat",
    "merch_long",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def predict_one(model, payload: dict) -> tuple[int, float]:
    df = pd.DataFrame([{k: payload[k] for k in REQUIRED}])
    pred = int(model.predict(df)[0])
    score = float(model.predict_proba(df)[0, 1])
    return pred, score


def main() -> None:
    log.info(f"Chargement modele : {MODEL_PATH}")
    model = joblib.load(MODEL_PATH)
    log.info("Modele charge.")

    log.info(f"Connexion Kafka {KAFKA_BOOTSTRAP} (group={GROUP_ID})")
    consumer = KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
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
                log.error(
                    f"Erreur prediction trans_num={payload.get('trans_num')} : {e}"
                )
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
                ground_truth=payload.get("ground_truth"),
                predicted_at=now_iso(),
            )

            producer.send(TOPIC_PRED, key=scored.trans_num, value=scored.model_dump())
            consumer.commit()

            n_processed += 1
            if is_fraud:
                n_fraud += 1
                log.warning(
                    f"FRAUDE | trans={scored.trans_num} | amt={scored.amt} | "
                    f"score={score:.3f} | truth={scored.ground_truth}"
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
