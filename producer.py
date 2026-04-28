"""
producer.py
===========
Producer Kafka : poll fake_api.py et publie dans le topic raw-transactions.

Responsabilites :
  1. Appeler /current-transactions toutes les POLL_INTERVAL secondes
  2. Valider chaque message via Pydantic
  3. Hash SHA-256 sur cc_num (RGPD)
  4. Publier dans le topic raw-transactions (clef = trans_num)
  5. Publier dans le topic dlq-raw si validation echoue
  6. Logger les metriques (publies / rejetes)

Usage :
  python producer.py

Variables d'environnement :
  API_URL              : URL de l'API source (defaut : http://fake-api:8000/current-transactions)
  KAFKA_BOOTSTRAP      : adresse du broker Kafka (defaut : localhost:9092)
  POLL_INTERVAL        : secondes entre deux pulls (defaut : 12)
  BATCH_SIZE           : nb de transactions par pull (defaut : 5)
  TOPIC_RAW            : topic principal (defaut : raw-transactions)
  TOPIC_DLQ            : topic DLQ (defaut : dlq-raw)
"""

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


# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

API_URL = os.getenv("API_URL", "http://localhost:8000/current-transactions")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "12"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
TOPIC_RAW = os.getenv("TOPIC_RAW", "raw-transactions")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dlq-raw")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][producer] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Pour ne logger qu'une seule fois le premier rejet de validation
_DLQ_FIRST_LOGGED = False


# ----------------------------------------------------------------------
# UTILITAIRES
# ----------------------------------------------------------------------

def hash_cc(cc_num: str) -> str:
    """SHA-256 hexdigest. Reversible uniquement avec rainbow table immense."""
    return hashlib.sha256(cc_num.encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ----------------------------------------------------------------------
# FETCH API
# ----------------------------------------------------------------------

FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.0"))


def fetch_batch(url: str, n: int, max_retries: int = 5) -> list[dict]:
    """Recupere un batch de transactions, retry exponentiel en cas d'echec."""
    params = {"n": n}
    if FRAUD_RATE > 0:
        params["fraud_rate"] = FRAUD_RATE
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            payload = r.json()

            # Format Pandas split : {columns: [...], data: [[...]]}
            if isinstance(payload, dict) and "columns" in payload and "data" in payload:
                cols = payload["columns"]
                rows = [dict(zip(cols, row)) for row in payload["data"]]
                return rows

            # Liste de dicts
            if isinstance(payload, list):
                return payload

            # Dict simple
            return [payload]
        except (requests.RequestException, ValueError) as e:
            wait = 2 ** attempt
            log.warning(f"Fetch attempt {attempt}/{max_retries} failed: {e}. Retry in {wait}s.")
            time.sleep(wait)

    log.error("Fetch impossible apres tous les retries. Skip ce cycle.")
    return []


# ----------------------------------------------------------------------
# KAFKA PRODUCER
# ----------------------------------------------------------------------

def make_producer() -> KafkaProducer:
    log.info(f"Connexion Kafka a {KAFKA_BOOTSTRAP}...")
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",            # Garantit que tous les replicas ont recu le message
        retries=5,
        linger_ms=100,         # Petit batching pour performance
    )


# ----------------------------------------------------------------------
# PIPELINE D'UN MESSAGE
# ----------------------------------------------------------------------

def process_message(producer: KafkaProducer, raw: dict) -> bool:
    """
    Valide, pseudonymise, et publie un message.
    Retourne True si publie en raw, False si DLQ.
    """
    # 1. Validation
    try:
        validated = RawTransaction(**raw)
    except ValidationError as e:
        # Log le premier rejet en detail, puis silencieux pour les suivants
        # (sinon les logs explosent en cas d'incident).
        global _DLQ_FIRST_LOGGED
        if not _DLQ_FIRST_LOGGED:
            _DLQ_FIRST_LOGGED = True
            log.warning(f"VALIDATION KO sur trans_num={raw.get('trans_num', '?')}")
            for err in e.errors()[:5]:
                log.warning(f"  -> field={err['loc']} type={err['type']} msg={err['msg']}")
            log.warning(f"  -> payload exemple = {raw}")
        dl = DeadLetter(
            rejected_at=now_iso(),
            error=str(e),
            raw_payload=raw,
        )
        producer.send(TOPIC_DLQ, key=raw.get("trans_num", "unknown"), value=dl.model_dump())
        return False

    # 2. Pseudonymisation cc_num (RGPD)
    payload = validated.model_dump()
    payload["cc_num_hash"] = hash_cc(payload.pop("cc_num"))
    payload["ingested_at"] = now_iso()

    # 3. Publication
    producer.send(TOPIC_RAW, key=payload["trans_num"], value=payload)
    return True


# ----------------------------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------------------------

def main() -> None:
    log.info(f"Demarrage producer | API={API_URL} | poll={POLL_INTERVAL}s | batch={BATCH_SIZE}")
    producer = make_producer()

    total_ok = 0
    total_dlq = 0

    try:
        while True:
            cycle_start = time.time()
            batch = fetch_batch(API_URL, BATCH_SIZE)

            n_ok = 0
            n_dlq = 0
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
            sleep_for = max(0, POLL_INTERVAL - elapsed)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log.info("Arret demande par l'utilisateur.")
    except KafkaError as e:
        log.error(f"Erreur Kafka fatale : {e}")
        raise
    finally:
        log.info(f"Fermeture | total publie={total_ok} | total DLQ={total_dlq}")
        producer.close()


if __name__ == "__main__":
    main()
