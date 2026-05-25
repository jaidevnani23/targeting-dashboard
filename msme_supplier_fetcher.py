#!/usr/bin/env python3
"""
MSME Supplier Fetcher
=====================
Fetches MSME registered units from data.gov.in API, filters by NIC codes
in data/Key_NIC_Codes_List.xlsx, maps categories from data/Demand_Excel_Filled.xlsx,
and outputs one suppliers_[State].json per state into data/suppliers/.

Adding new NIC codes to Key_NIC_Codes_List.xlsx automatically expands
what gets fetched and categorised — no code changes needed.

Run schedule: Quarterly (1st of Jan, Apr, Jul, Oct) via GitHub Actions.
Can also be run manually: python msme_supplier_fetcher.py

Requirements:
    pip install requests pandas openpyxl python-dotenv
"""

import requests
import pandas as pd
import os
import json
import time
import random
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY      = os.getenv("DATA_GOV_API_KEY", "579b464db66ec23bdd00000154d8f54213c049ed75275e408a101619")
RESOURCE_ID  = "2c1fd4a5-67c7-4672-a2c6-a0a76c2f00da"
BASE_URL     = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

NIC_CODES_FILE = "data/Key_NIC_Codes_List.xlsx"
DEMAND_FILE    = "data/Demand_Excel_Filled.xlsx"
OUTPUT_DIR     = "data/suppliers"

BATCH_SIZE       = 1000
TIMEOUT_SEC      = 120      # increased from 60 to handle slow API responses
MAX_RETRIES      = 5
MIN_BATCH_DELAY  = 6.77585  # minimum seconds between batches
MAX_BATCH_DELAY  = 9.84774  # maximum seconds between batches
STATE_DELAY_SEC  = 2        # increased from 0.5 to give API breathing room
MAX_PAGE_WORKERS = 1        # sequential page fetching — more reliable on gov API

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

STATES_AND_UTS = [
    "ANDAMAN AND NICOBAR ISLANDS", "ANDHRA PRADESH", "ARUNACHAL PRADESH",
    "ASSAM", "BIHAR", "CHANDIGARH", "CHHATTISGARH",
    "DADRA AND NAGAR HAVELI AND DAMAN AND DIU", "DELHI", "GOA", "GUJARAT",
    "HARYANA", "HIMACHAL PRADESH", "JAMMU AND KASHMIR", "JHARKHAND",
    "KARNATAKA", "KERALA", "LADAKH", "LAKSHADWEEP", "MADHYA PRADESH",
    "MAHARASHTRA", "MANIPUR", "MEGHALAYA", "MIZORAM", "NAGALAND", "ODISHA",
    "PUDUCHERRY", "PUNJAB", "RAJASTHAN", "SIKKIM", "TAMIL NADU", "TELANGANA",
    "TRIPURA", "UTTAR PRADESH", "UTTARAKHAND", "WEST BENGAL",
]

# ── LOAD REFERENCE FILES ──────────────────────────────────────────────────────
def load_nic_codes():
    df = pd.read_excel(NIC_CODES_FILE)
    code_col = next(c for c in df.columns if 'nic' in c.lower() and 'code' in c.lower())
    desc_col = next(c for c in df.columns if 'desc' in c.lower())
    df[code_col] = df[code_col].astype(str).str.strip()
    nic_set  = set(df[code_col].tolist())
    nic_desc = dict(zip(df[code_col], df[desc_col]))
    log.info(f"Loaded {len(nic_set)} NIC codes from {NIC_CODES_FILE}")
    return nic_set, nic_desc


def load_category_mapping():
    df = pd.read_excel(DEMAND_FILE)
    nic_col = next(c for c in df.columns if 'nic' in c.lower())
    cat_col = next(c for c in df.columns if 'cat' in c.lower())
    df[nic_col] = df[nic_col].astype(str).str.strip()
    mapping = (
        df.groupby(nic_col)[cat_col]
        .agg(lambda x: x.mode()[0])
        .to_dict()
    )
    log.info(f"Loaded {len(mapping)} NIC→Category mappings from {DEMAND_FILE}")
    return mapping


# ── RANDOM DELAY ──────────────────────────────────────────────────────────────
def random_batch_delay():
    """Sleep for a highly randomized duration between batches to avoid rate limiting"""
    delay = random.uniform(MIN_BATCH_DELAY, MAX_BATCH_DELAY)
    log.info(f"💤 Waiting {delay:.5f}s before next batch...")
    time.sleep(delay)


# ── HTTP SESSION ──────────────────────────────────────────────────────────────
_local = threading.local()

def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        retry = Retry(total=3, backoff_factor=2,
                      status_forcelist=[500, 502, 503, 504],
                      allowed_methods=["GET"])
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://",  HTTPAdapter(max_retries=retry))
        _local.session = s
    return _local.session


# ── API FETCH ─────────────────────────────────────────────────────────────────
def fetch_page(state: str, offset: int) -> dict:
    params = {
        "api-key":        API_KEY,
        "format":         "json",
        "limit":          BATCH_SIZE,
        "offset":         offset,
        "filters[State]": state,
    }
    session = get_session()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=TIMEOUT_SEC)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            wait = 30 * (2 ** (attempt - 1))   # 30s, 60s, 120s, 240s, 480s
            log.warning(f"[{state}] offset={offset} attempt {attempt}: {e}. Retry in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"[{state}] Failed at offset={offset} after {MAX_RETRIES} attempts.")


def fetch_all_for_state(state: str) -> pd.DataFrame:
    first = fetch_page(state, 0)
    total = int(first.get("total", 0))
    if total == 0:
        return pd.DataFrame()

    all_records = list(first.get("records", []))
    offsets     = list(range(BATCH_SIZE, total, BATCH_SIZE))
    log.info(f"[{state}] {total:,} records across {1 + len(offsets)} pages")

    if offsets:
        def _fetch(off, idx, total_pages):
            # Add random delay before each batch (except the first one which already happened)
            if idx > 0:
                random_batch_delay()
            
            result = fetch_page(state, off).get("records", [])
            log.info(f"[{state}] Fetched batch {idx + 2}/{total_pages + 1} (offset {off})")
            return result

        with ThreadPoolExecutor(max_workers=MAX_PAGE_WORKERS) as pool:
            futures = {pool.submit(_fetch, off, idx, len(offsets)): off 
                      for idx, off in enumerate(offsets)}
            for fut in as_completed(futures):
                all_records.extend(fut.result())

    return pd.DataFrame(all_records)


# ── FILTER + FORMAT ───────────────────────────────────────────────────────────
def process_state(state: str, nic_set: set, nic_desc: dict, cat_map: dict) -> list:
    df = fetch_all_for_state(state)
    if df.empty:
        return []

    # Try Activities column first (nested NIC codes), fall back to flat NIC column
    activities_col = next((c for c in df.columns if 'activit' in c.lower()), None)
    nic_col        = next((c for c in df.columns if 'nic' in c.lower()), None)

    results = []

    if activities_col:
        # Activities column contains a JSON array of NIC codes per enterprise
        for _, row in df.iterrows():
            raw = row.get(activities_col, "[]")
            try:
                activities = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except (json.JSONDecodeError, TypeError):
                activities = []

            for activity in activities:
                if not isinstance(activity, dict):
                    continue
                nic_code = str(activity.get("NIC5DigitId", "")).strip()
                if nic_code not in nic_set:
                    continue
                results.append({
                    "State":           str(row.get("State", state)).strip().title(),
                    "District":        str(row.get("District", "")).strip().title(),
                    "Pincode":         str(row.get("Pincode", "")).strip(),
                    "EnterpriseName":  str(row.get("EnterpriseName", "")).strip().title(),
                    "NIC_Code":        nic_code,
                    "NIC_Description": activity.get("Description", nic_desc.get(nic_code, "")),
                    "Category":        cat_map.get(nic_code, "Uncategorised"),
                })

    elif nic_col:
        # Flat NIC code column — simpler format
        df[nic_col] = df[nic_col].astype(str).str.strip()
        filtered    = df[df[nic_col].isin(nic_set)].copy()
        for _, row in filtered.iterrows():
            nic_code = str(row.get(nic_col, "")).strip()
            results.append({
                "State":           str(row.get("State", state)).strip().title(),
                "District":        str(row.get("District", "")).strip().title(),
                "Pincode":         str(row.get("Pincode", "")).strip(),
                "EnterpriseName":  str(row.get("EnterpriseName", "")).strip().title(),
                "NIC_Code":        nic_code,
                "NIC_Description": nic_desc.get(nic_code, str(row.get("NIC5DigitCode", ""))),
                "Category":        cat_map.get(nic_code, "Uncategorised"),
            })

    else:
        log.warning(f"[{state}] No NIC code column found. Skipping.")
        return []

    log.info(f"[{state}] {len(results):,} matching rows (from {len(df):,} total)")
    return results


# ── SAVE ──────────────────────────────────────────────────────────────────────
def save_state_json(state: str, records: list):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_name = state.title().replace(" ", "_")
    path      = os.path.join(OUTPUT_DIR, f"suppliers_{safe_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    log.info(f"[{state}] Saved {len(records):,} suppliers → {path}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("MSME Supplier Fetcher — Quarterly Run")
    log.info("=" * 60)

    nic_set, nic_desc = load_nic_codes()
    cat_map           = load_category_mapping()

    total_suppliers = 0
    failed_states   = []

    for i, state in enumerate(STATES_AND_UTS, 1):
        log.info(f"\n[{i:02d}/{len(STATES_AND_UTS)}] Processing: {state.title()}")
        try:
            records = process_state(state, nic_set, nic_desc, cat_map)
            if records:
                save_state_json(state, records)
                total_suppliers += len(records)
            else:
                log.info(f"[{state}] No matching suppliers — skipping file.")
        except Exception as e:
            log.error(f"[{state}] FAILED: {e}")
            failed_states.append(state)

        if i < len(STATES_AND_UTS):
            time.sleep(STATE_DELAY_SEC)

    log.info("\n" + "=" * 60)
    log.info(f"Total suppliers saved : {total_suppliers:,}")
    log.info(f"Output folder         : {OUTPUT_DIR}/")
    if failed_states:
        log.info("Failed states:")
        for s in failed_states:
            log.info(f"  - {s.title()}")
    else:
        log.info("All states completed successfully.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
