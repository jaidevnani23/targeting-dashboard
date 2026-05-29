#!/usr/bin/env python3
"""
MSME Supplier Fetcher
=====================
Fetches MSME registered units from data.gov.in API, filters by NIC codes
in data/Key_NIC_Codes_List.xlsx, maps categories from data/Demand_Excel_Filled.xlsx,
and outputs one suppliers_[State].csv per state into data/suppliers/.

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
API_KEY = os.getenv("DATA_GOV_API_KEY")
if not API_KEY:
    raise EnvironmentError("DATA_GOV_API_KEY not set in environment or .env file")
RESOURCE_ID  = "list-msme-registered-units-under-udyam"
BASE_URL     = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

NIC_CODES_FILE  = "data/Key_NIC_Codes_List.xlsx"
DEMAND_FILE     = "data/Demand_Excel_Filled.xlsx"
OUTPUT_DIR      = "data/suppliers"

CHECKPOINT_FILE = "data/suppliers/fetch_checkpoint.json"

BATCH_SIZE       = 100
TIMEOUT_SEC      = 120
MAX_RETRIES      = 5
MIN_BATCH_DELAY  = 6.77585
MAX_BATCH_DELAY  = 9.84774
STATE_DELAY_SEC  = 2
MAX_PAGE_WORKERS = 1

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

# ── FIX 1: ROBUST COLUMN DETECTION ───────────────────────────────────────────
def find_column(df_columns, *keywords):
    """
    Case-insensitive search for a column whose name contains ANY of the
    given keywords. Returns the first match, or None if nothing found.
    Logs every candidate it sees so you can debug API column name changes.
    """
    cols_lower = {c.lower(): c for c in df_columns}
    for kw in keywords:
        for lower, original in cols_lower.items():
            if kw.lower() in lower:
                return original
    return None


def detect_nic_columns(df):
    """
    Returns (activities_col, flat_nic_col) — at most one will be non-None.
    Logs the actual column list so you can see exactly what the API returned.
    """
    log.info(f"  API columns: {list(df.columns)}")

    activities_col = find_column(df.columns,
                                 'activit', 'activities', 'activity')

    flat_nic_col = find_column(df.columns,
                               'nic5digit', 'nic_code', 'niccode',
                               'nic5', 'nic')

    log.info(f"  Detected → activities_col={activities_col!r}, flat_nic_col={flat_nic_col!r}")
    return activities_col, flat_nic_col


# ── CHECKPOINT ────────────────────────────────────────────────────────────────
def load_checkpoint() -> dict:
    if not os.path.exists(CHECKPOINT_FILE):
        return {"completed": [], "failed": []}
    try:
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            data = json.load(f)
        log.info(f"Checkpoint loaded — {len(data.get('completed', []))} states already done, "
                 f"{len(data.get('failed', []))} previously failed")
        return data
    except Exception as e:
        log.warning(f"Could not read checkpoint file: {e}. Starting fresh.")
        return {"completed": [], "failed": []}


def save_checkpoint(checkpoint: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint cleared — clean run complete.")


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
            wait = 30 * (2 ** (attempt - 1))
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

    for idx, off in enumerate(offsets):
        random_batch_delay()
        records = fetch_page(state, off).get("records", [])
        all_records.extend(records)
        log.info(f"[{state}] Fetched page {idx + 2}/{1 + len(offsets)} (offset {off})")

    return pd.DataFrame(all_records)


# ── FILTER + FORMAT ───────────────────────────────────────────────────────────
def process_state(state: str, nic_set: set, nic_desc: dict, cat_map: dict) -> list:
    df = fetch_all_for_state(state)
    if df.empty:
        log.info(f"[{state}] No records returned from API.")
        return []

    activities_col, flat_nic_col = detect_nic_columns(df)

    results = []

    if activities_col:
        for _, row in df.iterrows():
            raw = row.get(activities_col, "[]")
            try:
                activities = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except (json.JSONDecodeError, TypeError):
                activities = []

            for activity in activities:
                if not isinstance(activity, dict):
                    continue
                nic_code = (
                    str(activity.get("NIC5DigitId", "") or
                        activity.get("NIC5DigitCode", "") or
                        activity.get("nic5digitid", "") or
                        activity.get("NICCode", "") or
                        "").strip()
                )
                if not nic_code or nic_code not in nic_set:
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

    elif flat_nic_col:
        for _, row in df.iterrows():
            raw_nic = str(row.get(flat_nic_col, "")).strip()
            if not raw_nic or raw_nic == 'nan':
                continue

            # Parse " 1) 14101; 2) 22199; 3) 32909" → ["14101", "22199", "32909"]
            codes = []
            for part in raw_nic.split(';'):
                code = part.strip()
                if ')' in code:
                    code = code.split(')')[-1].strip()
                if code and code != 'nan':
                    codes.append(code)

            for code in codes:
                if code not in nic_set:
                    continue
                results.append({
                    "State":           str(row.get("State", state)).strip().title(),
                    "District":        str(row.get("District", "")).strip().title(),
                    "Pincode":         str(row.get("Pincode", "")).strip(),
                    "EnterpriseName":  str(row.get("EnterpriseName", "")).strip().title(),
                    "NIC_Code":        code,
                    "NIC_Description": nic_desc.get(code, ""),
                    "Category":        cat_map.get(code, "Uncategorised"),
                })

    else:
        log.error(
            f"[{state}] Could not find a NIC code column. "
            f"API returned these columns: {list(df.columns)}. "
            f"Update the keywords in find_column() to match one of these."
        )
        return []

    log.info(f"[{state}] {len(results):,} matching rows (from {len(df):,} total)")
    return results


# ── SAVE ──────────────────────────────────────────────────────────────────────
def save_state_csv(state: str, records: list):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_name = state.title().replace(" ", "_")
    path      = os.path.join(OUTPUT_DIR, f"suppliers_{safe_name}.csv")
    pd.DataFrame(records).to_csv(path, index=False, encoding="utf-8-sig")
    log.info(f"[{state}] Saved {len(records):,} suppliers → {path}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("MSME Supplier Fetcher — Quarterly Run")
    log.info("=" * 60)

    nic_set, nic_desc = load_nic_codes()
    cat_map           = load_category_mapping()

    checkpoint    = load_checkpoint()
    completed_set = set(checkpoint.get("completed", []))
    failed_states = list(checkpoint.get("failed", []))

    remaining = [s for s in STATES_AND_UTS if s not in completed_set]
    if len(remaining) < len(STATES_AND_UTS):
        skipped = len(STATES_AND_UTS) - len(remaining)
        log.info(f"Resuming — skipping {skipped} already-completed state(s), "
                 f"{len(remaining)} remaining.")

    total_suppliers = 0

    for i, state in enumerate(remaining, 1):
        log.info(f"\n[{i:02d}/{len(remaining)}] Processing: {state.title()}")
        try:
            records = process_state(state, nic_set, nic_desc, cat_map)
            if records:
                save_state_csv(state, records)
                total_suppliers += len(records)
            else:
                log.info(f"[{state}] No matching suppliers — skipping file.")

            completed_set.add(state)
            failed_states = [s for s in failed_states if s != state]
            checkpoint = {"completed": sorted(completed_set), "failed": failed_states}
            save_checkpoint(checkpoint)

        except Exception as e:
            log.error(f"[{state}] FAILED: {e}")
            if state not in failed_states:
                failed_states.append(state)
            checkpoint = {"completed": sorted(completed_set), "failed": failed_states}
            save_checkpoint(checkpoint)

        if i < len(remaining):
            time.sleep(STATE_DELAY_SEC)

    log.info("\n" + "=" * 60)
    log.info(f"Total suppliers saved : {total_suppliers:,}")
    log.info(f"Output folder         : {OUTPUT_DIR}/")

    if failed_states:
        log.info("Failed states (will be retried on next run):")
        for s in failed_states:
            log.info(f"  - {s.title()}")
    else:
        log.info("All states completed successfully.")
        if not (set(STATES_AND_UTS) - completed_set):
            clear_checkpoint()

    log.info("=" * 60)


if __name__ == "__main__":
    main()
