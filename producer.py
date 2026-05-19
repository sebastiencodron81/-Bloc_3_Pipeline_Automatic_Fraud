"""Producer Kafka : poll l'API source et publie dans raw-transactions."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import ValidationError

from schemas import DeadLetter, RawTransaction


API_URL = os.getenv(
    "API_URL",
    "https://sdacelo-real-time-fraud-detection.hf.space/current-transactions",
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "3"))
TOPIC_RAW = os.getenv("TOPIC_RAW", "raw-transactions")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dlq-raw")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][producer] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_DLQ_FIRST_LOGGED = False


def hash_cc(cc_num: str) -> str:
    return hashlib.sha256(str(cc_num).encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_transactions(url: str, max_retries: int = 5) -> list[dict]:
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, str):
                payload = json.loads(payload)

            if isinstance(payload, dict) and "columns" in payload and "data" in payload:
                cols = payload["columns"]
                return [dict(zip(cols, row)) for row in payload["data"]]

            if isinstance(payload, list):
                return payload

            return [payload]

        except (requests.RequestException, ValueError) as e:
            wait = 2 ** attempt
            log.warning(
                f"Fetch attempt {attempt}/{max_retries} failed: {e}. Retry in {wait}s."
            )
            time.sleep(wait)

    log.error("Fetch impossible apres tous les retries. Skip ce cycle.")
    return []


def make_producer() -> KafkaProducer:
    log.info(f"Connexion Kafka : {KAFKA_BOOTSTRAP}")
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=100,
    )


def process_message(producer: KafkaProducer, raw: dict) -> bool:
    global _DLQ_FIRST_LOGGED

    try:
        validated = RawTransaction(**raw)
    except ValidationError as e:
        if not _DLQ_FIRST_LOGGED:
            _DLQ_FIRST_LOGGED = True
            log.warning(f"Validation KO trans_num={raw.get('trans_num', '?')}")
            for err in e.errors()[:3]:
                log.warning(f"  field={err['loc']} type={err['type']} msg={err['msg']}")
        dl = DeadLetter(
            rejected_at=now_iso(),
            error=str(e),
            raw_payload=raw,
        )
        producer.send(
            TOPIC_DLQ,
            key=str(raw.get("trans_num", "unknown")),
            value=dl.model_dump(),
        )
        return False

    payload = validated.model_dump()
    payload["cc_num_hash"] = hash_cc(payload.pop("cc_num"))

    ts = datetime.fromtimestamp(payload["current_time"] / 1000, tz=timezone.utc).replace(tzinfo=None)
    payload["trans_date_trans_time"] = ts.isoformat()

    if payload.get("is_fraud") is not None:
        payload["ground_truth"] = int(payload["is_fraud"])
    payload.pop("is_fraud", None)

    payload["ingested_at"] = now_iso()

    producer.send(TOPIC_RAW, key=payload["trans_num"], value=payload)
    return True


def main() -> None:
    log.info(f"Demarrage producer | API={API_URL} | poll={POLL_INTERVAL}s")
    producer = make_producer()

    total_ok = 0
    total_dlq = 0

    try:
        while True:
            cycle_start = time.time()
            batch = fetch_transactions(API_URL)

            n_ok = n_dlq = 0
            for raw in batch:
                if process_message(producer, raw):
                    n_ok += 1
                else:
                    n_dlq += 1

            producer.flush()
            total_ok += n_ok
            total_dlq += n_dlq

            log.info(
                f"Cycle | publie={n_ok}/{len(batch)} | DLQ={n_dlq} | "
                f"total={total_ok} | total_DLQ={total_dlq}"
            )

            elapsed = time.time() - cycle_start
            time.sleep(max(0, POLL_INTERVAL - elapsed))

    except KeyboardInterrupt:
        log.info("Arret demande.")
    except KafkaError as e:
        log.error(f"Erreur Kafka : {e}")
        raise
    finally:
        log.info(f"Fermeture | total={total_ok} | DLQ={total_dlq}")
        producer.close()


if __name__ == "__main__":
    main()
