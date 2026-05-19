"""Consumer Kafka : persiste les predictions dans PostgreSQL."""

from __future__ import annotations

import json
import logging
import os

from kafka import KafkaConsumer
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_PRED = os.getenv("TOPIC_PREDICTIONS", "fraud-predictions")
GROUP_ID = os.getenv("GROUP_ID", "storage-consumer-group")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://fraud:fraud@localhost:5432/fraud",
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][storage] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


metadata = MetaData()
payments_gold = Table(
    "payments_gold",
    metadata,
    Column("trans_num", String, primary_key=True),
    Column("cc_num_hash", String, nullable=False),
    Column("trans_date_trans_time", DateTime, nullable=False),
    Column("merchant", String),
    Column("category", String),
    Column("amt", Numeric),
    Column("state", String(2)),
    Column("city", String),
    Column("is_fraud", SmallInteger),
    Column("score", Numeric),
    Column("ground_truth", SmallInteger),
    Column("predicted_at", DateTime),
    Column("ingested_at", DateTime, server_default=func.now()),
)


def main() -> None:
    log.info(f"Connexion Postgres : {DATABASE_URL}")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    metadata.create_all(engine)
    log.info("Schema verifie.")

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
            stmt = pg_insert(payments_gold).values(**payload)
            stmt = stmt.on_conflict_do_update(
                index_elements=["trans_num"],
                set_={
                    "is_fraud": stmt.excluded.is_fraud,
                    "score": stmt.excluded.score,
                    "ground_truth": stmt.excluded.ground_truth,
                    "predicted_at": stmt.excluded.predicted_at,
                },
            )
            with engine.begin() as conn:
                conn.execute(stmt)
            consumer.commit()

            n += 1
            if n % 25 == 0:
                log.info(f"Persiste {n} transactions.")

    except KeyboardInterrupt:
        log.info("Arret demande.")
    finally:
        consumer.close()
        log.info(f"Fermeture | total={n}")


if __name__ == "__main__":
    main()
