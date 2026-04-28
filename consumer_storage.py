"""
consumer_storage.py
===================
Consumer Kafka qui persiste TOUTES les predictions (fraudes et non-fraudes)
dans PostgreSQL, table payments_gold.

C'est cette table qui alimente :
  - le rapport quotidien (cron 6h matin)
  - la vue historique du dashboard Streamlit

Schema cible :
  CREATE TABLE payments_gold (
      trans_num             VARCHAR PRIMARY KEY,
      cc_num_hash           VARCHAR NOT NULL,
      trans_date_trans_time TIMESTAMP NOT NULL,
      merchant              VARCHAR,
      category              VARCHAR,
      amt                   NUMERIC,
      state                 VARCHAR(2),
      city                  VARCHAR,
      is_fraud              SMALLINT,
      score                 NUMERIC,
      predicted_at          TIMESTAMP,
      ingested_at           TIMESTAMP DEFAULT NOW()
  );
"""

from __future__ import annotations

import json
import logging
import os

from kafka import KafkaConsumer
from sqlalchemy import (
    Column, DateTime, Integer, Numeric, MetaData, String, Table,
    create_engine, func,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_PRED = os.getenv("TOPIC_PREDICTIONS", "fraud-predictions")
GROUP_ID = os.getenv("GROUP_ID", "storage-consumer-group")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://fraud:fraud@localhost:5432/fraud"
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][storage] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# SCHEMA TABLE
# ----------------------------------------------------------------------

metadata = MetaData()
payments_gold = Table(
    "payments_gold", metadata,
    Column("trans_num", String, primary_key=True),
    Column("cc_num_hash", String, nullable=False),
    Column("trans_date_trans_time", DateTime, nullable=False),
    Column("merchant", String),
    Column("category", String),
    Column("amt", Numeric),
    Column("state", String(2)),
    Column("city", String),
    Column("is_fraud", Integer),
    Column("score", Numeric),
    Column("predicted_at", DateTime),
    Column("ingested_at", DateTime, server_default=func.now()),
)


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main() -> None:
    log.info(f"Connexion Postgres : {DATABASE_URL}")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    metadata.create_all(engine)
    log.info("Schema verifie/cree.")

    log.info(f"Connexion Kafka {KAFKA_BOOTSTRAP} (group={GROUP_ID})")
    consumer = KafkaConsumer(
        TOPIC_PRED,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    n = 0
    try:
        for msg in consumer:
            payload = msg.value

            # Upsert pour idempotence : meme trans_num ne cree pas de doublon
            stmt = pg_insert(payments_gold).values(**payload)
            stmt = stmt.on_conflict_do_update(
                index_elements=["trans_num"],
                set_={
                    "is_fraud": stmt.excluded.is_fraud,
                    "score": stmt.excluded.score,
                    "predicted_at": stmt.excluded.predicted_at,
                },
            )

            # IMPORTANT : une transaction par message (commit immediat).
            # Avant on avait un engine.begin() autour de toute la boucle
            # -> aucune ligne visible avant que la boucle ne se termine.
            with engine.begin() as conn:
                conn.execute(stmt)

            consumer.commit()

            n += 1
            if n % 50 == 0:
                log.info(f"Persiste {n} transactions.")

    except KeyboardInterrupt:
        log.info("Arret demande.")
    finally:
        consumer.close()
        log.info(f"Fermeture | total persiste={n}")


if __name__ == "__main__":
    main()
