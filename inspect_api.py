"""Diagnostic du contrat de l'API source : compare ses colonnes au CSV de reference."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests


API_URL = "https://sdacelo-real-time-fraud-detection.hf.space/current-transactions"
CSV_PATH = Path(__file__).parent / "fraudTest.csv"
OUT_SAMPLE = Path(__file__).parent / "api_sample.json"
OUT_REPORT = Path(__file__).parent / "api_report.md"

SAMPLE_SIZE = 5

PII_OR_LEAKY = {
    "cc_num",
    "first",
    "last",
    "street",
    "trans_num",
    "unix_time",
    "dob",
}

TARGET = "is_fraud"


def fetch_api(url: str, timeout: int = 15, max_retries: int = 3) -> dict | list:
    print(f"[1/4] Appel : {url}")
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, str):
                payload = json.loads(payload)
            print(f"      OK (status {r.status_code}, {len(r.content)} bytes)")
            return payload
        except (requests.RequestException, ValueError) as e:
            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"      Tentative {attempt}/{max_retries} echouee. Retry dans {wait}s.")
                time.sleep(wait)
            else:
                sys.exit(f"\nERREUR : {e}")


def normalize_to_dataframe(payload) -> pd.DataFrame:
    print("[2/4] Normalisation en DataFrame...")
    if isinstance(payload, dict) and "columns" in payload and "data" in payload:
        df = pd.DataFrame(payload["data"], columns=payload["columns"])
    elif isinstance(payload, list):
        df = pd.DataFrame(payload)
    else:
        df = pd.DataFrame([payload])
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    print(f"      DataFrame API : {df.shape[0]} lignes x {df.shape[1]} colonnes")
    return df


def load_csv(path: Path, n: int = 5_000) -> pd.DataFrame:
    print(f"[3/4] Chargement CSV : {path.name}")
    if not path.exists() or path.stat().st_size < 1024:
        sys.exit(f"\nERREUR : CSV manquant ou vide a {path}")
    df = pd.read_csv(path, nrows=n)
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    print(f"      DataFrame CSV : {df.shape[0]} lignes (sample) x {df.shape[1]} colonnes")
    return df


def compare_schemas(df_api: pd.DataFrame, df_csv: pd.DataFrame) -> dict:
    cols_api = set(df_api.columns)
    cols_csv = set(df_csv.columns)
    common = cols_csv & cols_api
    only_csv = cols_csv - cols_api
    only_api = cols_api - cols_csv
    usable = common - PII_OR_LEAKY - {TARGET}
    return {
        "common": sorted(common),
        "only_csv": sorted(only_csv),
        "only_api": sorted(only_api),
        "usable_for_training": sorted(usable),
        "excluded_pii": sorted(common & PII_OR_LEAKY),
    }


def write_outputs(diff, df_api, df_csv, payload) -> None:
    print("[4/4] Generation des sorties...")
    OUT_SAMPLE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    md = []
    md.append("# Rapport de comparaison API <-> CSV\n")
    md.append(f"- Source API : `{API_URL}`")
    md.append(f"- Source CSV : `{CSV_PATH.name}`\n")
    md.append("## Dimensions\n")
    md.append(f"- API : **{df_api.shape[0]}** lignes, **{df_api.shape[1]}** colonnes")
    md.append(f"- CSV : **{df_csv.shape[0]}** lignes (sample), **{df_csv.shape[1]}** colonnes\n")
    md.append("## Colonnes communes\n")
    for c in diff["common"]:
        md.append(f"- `{c}`")
    md.append("\n## Colonnes uniquement dans le CSV\n")
    for c in diff["only_csv"] or ["_(aucune)_"]:
        md.append(f"- `{c}`")
    md.append("\n## Colonnes uniquement dans l'API\n")
    for c in diff["only_api"] or ["_(aucune)_"]:
        md.append(f"- `{c}`")
    md.append("\n## Colonnes exclues (PII / leaky)\n")
    for c in diff["excluded_pii"]:
        md.append(f"- `{c}`")
    md.append("\n## Features utilisables pour l'entrainement ML\n")
    for c in diff["usable_for_training"]:
        md.append(f"- `{c}`")
    OUT_REPORT.write_text("\n".join(md), encoding="utf-8")


def print_summary(diff) -> None:
    print()
    print("=" * 70)
    print("RESUME")
    print("=" * 70)
    print(f"Communes              : {len(diff['common'])}")
    print(f"Uniquement CSV        : {len(diff['only_csv'])} -> {diff['only_csv']}")
    print(f"Uniquement API        : {len(diff['only_api'])} -> {diff['only_api']}")
    print(f"Exclues (PII/leaky)   : {len(diff['excluded_pii'])} -> {diff['excluded_pii']}")
    print()
    print(f"Features utilisables  : {len(diff['usable_for_training'])}")
    for c in diff["usable_for_training"]:
        print(f"  - {c}")
    print()
    print(f"Detail brut API       : {OUT_SAMPLE.name}")
    print(f"Rapport markdown      : {OUT_REPORT.name}")


def main() -> None:
    payload = fetch_api(API_URL)
    df_api = normalize_to_dataframe(payload)
    df_csv = load_csv(CSV_PATH)
    diff = compare_schemas(df_api, df_csv)
    write_outputs(diff, df_api, df_csv, payload)
    print_summary(diff)


if __name__ == "__main__":
    main()
