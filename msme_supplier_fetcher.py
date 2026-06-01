"""
MSME Supplier Fetcher  —  Production v3
========================================
Fetches MSME registered units from data.gov.in, filters by NIC codes
defined in data/Key_NIC_Codes_List.xlsx, maps categories from
data/Demand_Excel_Filled.xlsx, and writes one
    data/suppliers/suppliers_<State>.csv
per state.

Rate limit: data.gov.in allows 1,000 requests/hour (rolling window).
This script enforces a 15.0s minimum gap at 100 records/page →
~240 req/hr, giving substantial headroom.

Exit codes (read by GitHub Actions):
    0  — all states completed cleanly
    2  — states still pending (timeout/partial); workflow auto-retriggers

FIXES vs v2
-----------
FIX 5 — Correct API response schema.
    The API returns 9 columns:
        LG_ST_Code, State, LG_DT_Code, District, Pincode,
        RegistrationDate, EnterpriseName, CommunicationAddress, Activities
    EnterpriseType and MajorActivity do NOT exist in the response.
    NIC codes live in the 'Activities' column as a JSON array:
        [{"NIC5DigitId":"14101","Description":"Manufacture of ..."}]
    Previous code looked for columns with "nic" in the name — none exist.
    _find_nic_column() is replaced with a constant; _extract_nic_codes()
    now parses the JSON array format (with CSV double-quote escaping).

FIX 6 — Batch size reduced to 100, gap increased to 15s.
    Per user testing, 100 records/page is reliable.
    15s gap → ~240 req/hr, well within the 1,000/hr limit.

FIX 1 — (retained) urllib3 Retry excludes 429 from status_forcelist.
FIX 2 — (retained) fallback blind pagination when total==0.
FIX 3 — (removed, superseded by FIX 5) NIC column detection.
FIX 4 — (retained) 429 backoff: 120 + 60*attempt seconds.

Requirements:
    pip install requests pandas openpyxl

Usage:
    python msme_supplier_fetcher.py               # normal run / resume
    python msme_supplier_fetcher.py --reset       # ignore checkpoint
    python msme_supplier_fetcher.py --state DELHI # single state
    python msme_supplier_fetcher.py --dry-run     # fetch+filter, no writes
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from typing import Optional

import pandas as pd
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────
API_KEY = os.environ.get(
    "DATA_GOV_API_KEY",
    "579b464db66ec23bdd0000015260684b497743176979a5132577de55",
)
RESOURCE_ID = os.environ.get(
    "DATA_GOV_RESOURCE_ID",
    "8b68ae56-84cf-4728-a0a6-1be11028dea7",
)
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

# Paths work for both local (flat) and GitHub (data/ subfolder) layouts
_BASE = "data" if os.path.isdir("data") else "."
NIC_CODES_FILE = os.path.join(_BASE, "Key_NIC_Codes_List.xlsx")
DEMAND_FILE    = os.path.join(_BASE, "Demand_Excel_Filled.xlsx")
OUTPUT_DIR     = os.path.join(_BASE, "suppliers")
CHECKPOINT     = os.path.join(OUTPUT_DIR, "fetch_checkpoint.json")

# FIX 6: 1000 records/page, 3s gap
BATCH_SIZE    = 1000   # records per API page (reliable per live testing)
TIMEOUT_PAGE  = 60     # per-request socket timeout (high for GitHub Actions → Indian govt API latency)
MAX_RETRIES   = 4      # application-level retry attempts
RETRY_BASE    = 5      # urllib3 backoff base (seconds)

# FIX 6: 4.5s gap → ~800 req/hr (limit is 1,000/hr, ~20% headroom)
MIN_REQUEST_GAP: float = 4.5
_last_request_at: float = 0.0

# FIX 5: The NIC codes are in the 'Activities' column — always.
# This is a constant, not something to detect dynamically.
ACTIVITIES_COLUMN = "Activities"

# ─────────────────────────────────────────────────────────────────────────────
#  STATES / UTs  (36 total)
# ─────────────────────────────────────────────────────────────────────────────
STATES_AND_UTS: list[str] = [
    "ANDAMAN AND NICOBAR ISLANDS",
    "ANDHRA PRADESH",
    "ARUNACHAL PRADESH",
    "ASSAM",
    "BIHAR",
    "CHANDIGARH",
    "CHHATTISGARH",
    "DADRA AND NAGAR HAVELI AND DAMAN AND DIU",
    "DELHI",
    "GOA",
    "GUJARAT",
    "HARYANA",
    "HIMACHAL PRADESH",
    "JAMMU AND KASHMIR",
    "JHARKHAND",
    "KARNATAKA",
    "KERALA",
    "LADAKH",
    "LAKSHADWEEP",
    "MADHYA PRADESH",
    "MAHARASHTRA",
    "MANIPUR",
    "MEGHALAYA",
    "MIZORAM",
    "NAGALAND",
    "ODISHA",
    "PUDUCHERRY",
    "PUNJAB",
    "RAJASTHAN",
    "SIKKIM",
    "TAMIL NADU",
    "TELANGANA",
    "TRIPURA",
    "UTTAR PRADESH",
    "UTTARAKHAND",
    "WEST BENGAL",
]

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  HTTP SESSION
#  FIX 1: 429 excluded from status_forcelist — handled in app layer only.
# ─────────────────────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/csv",
    })
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],   # 429 intentionally excluded
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s


SESSION = _build_session()

# ─────────────────────────────────────────────────────────────────────────────
#  RATE-LIMITER
# ─────────────────────────────────────────────────────────────────────────────
def _throttle() -> None:
    global _last_request_at
    elapsed = time.monotonic() - _last_request_at
    gap = MIN_REQUEST_GAP - elapsed
    if gap > 0:
        time.sleep(gap)
    _last_request_at = time.monotonic()


# ─────────────────────────────────────────────────────────────────────────────
#  NO JSON PROBE — pure blind CSV pagination
#  The JSON endpoint times out reliably. All calls use format=csv.
#  process_state() simply fetches pages until it gets an empty one.
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
#  FETCH ONE CSV PAGE
# ─────────────────────────────────────────────────────────────────────────────
def fetch_page_csv(state: str, offset: int) -> list[dict]:
    """
    Fetch one batch of records as CSV. Returns [] on empty page (end of data).
    Raises RuntimeError after MAX_RETRIES failures.
    FIX 4: 429 backoff is 120 + 60*attempt seconds.
    """
    params = {
        "api-key":        API_KEY,
        "format":         "csv",
        "limit":          BATCH_SIZE,
        "offset":         offset,
        "filters[State]": state,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        _throttle()
        try:
            resp = SESSION.get(BASE_URL, params=params, timeout=TIMEOUT_PAGE, verify=False)

            if resp.status_code == 429:
                wait = 120 + (60 * attempt)   # FIX 4
                log.warning(
                    f"[{state}] offset={offset} → 429 rate-limited. "
                    f"Waiting {wait}s ({wait/60:.1f} min) "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            text = resp.text.strip()

            if not text:
                log.debug(f"[{state}] offset={offset} — empty body, end of data.")
                return []

            reader = csv.DictReader(io.StringIO(text))
            rows   = list(reader)

            if not rows:
                log.debug(f"[{state}] offset={offset} — header-only CSV, end of data.")
                return []

            if offset == 0:
                log.info(f"[{state}] CSV columns: {list(rows[0].keys())}")

            return rows

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        ) as exc:
            wait = RETRY_BASE * (2 ** (attempt - 1))
            log.warning(
                f"[{state}] offset={offset} attempt {attempt}/{MAX_RETRIES}: "
                f"{exc}. Retrying in {wait}s"
            )
            time.sleep(wait)

    raise RuntimeError(
        f"[{state}] offset={offset} — failed after {MAX_RETRIES} attempts."
    )


# ─────────────────────────────────────────────────────────────────────────────
#  REFERENCE FILE LOADERS
# ─────────────────────────────────────────────────────────────────────────────
def load_nic_codes() -> tuple[set[str], dict[str, str]]:
    df = pd.read_excel(NIC_CODES_FILE, dtype=str)
    df.columns = df.columns.str.strip()

    code_col = (
        next((c for c in df.columns if "nic" in c.lower() and "code" in c.lower()), None)
        or next((c for c in df.columns if "nic" in c.lower()), None)
    )
    if code_col is None:
        raise ValueError(
            f"No NIC code column found in {NIC_CODES_FILE}. "
            f"Columns: {list(df.columns)}"
        )

    desc_col = next((c for c in df.columns if "desc" in c.lower()), None)

    df[code_col] = df[code_col].str.strip().str.zfill(5)
    nic_set  = set(df[code_col].dropna().tolist())
    nic_desc = (
        dict(zip(df[code_col], df[desc_col].fillna(""))) if desc_col else {}
    )

    log.info(f"Loaded {len(nic_set)} NIC codes from {NIC_CODES_FILE} (col: {code_col!r})")
    return nic_set, nic_desc


def load_category_mapping() -> dict[str, str]:
    all_sheets = pd.read_excel(DEMAND_FILE, sheet_name=None)
    df = nic_col = cat_col = None

    for sheet_name, sheet_df in all_sheets.items():
        sheet_df.columns = sheet_df.columns.str.strip()
        _nic = next((c for c in sheet_df.columns if "nic" in c.lower()), None)
        _cat = next((c for c in sheet_df.columns if "cat" in c.lower()), None)
        if _nic and _cat:
            df, nic_col, cat_col = sheet_df, _nic, _cat
            log.info(
                f"Category mapping: sheet {sheet_name!r} "
                f"(cols: {nic_col!r}, {cat_col!r})"
            )
            break

    if df is None:
        raise ValueError(
            f"No NIC/Category columns found in any sheet of {DEMAND_FILE}."
        )

    def _norm(x) -> Optional[str]:
        s = str(x).strip()
        if not s or s == "nan":
            return None
        try:
            return str(int(float(s))).zfill(5)
        except (ValueError, OverflowError):
            return None

    df[nic_col] = df[nic_col].apply(_norm)
    mapping = (
        df.dropna(subset=[nic_col, cat_col])
        .groupby(nic_col)[cat_col]
        .agg(lambda x: x.mode().iloc[0])
        .to_dict()
    )

    log.info(f"Loaded {len(mapping)} NIC→Category mappings from {DEMAND_FILE}")
    return mapping


# ─────────────────────────────────────────────────────────────────────────────
#  NIC CODE EXTRACTION  (FIX 5)
#
#  The API returns NIC codes in the 'Activities' column as a JSON array:
#      [{"NIC5DigitId":"14101","Description":"Manufacture of ..."}]
#
#  CSV double-quote escaping means embedded quotes appear as "":
#      "[{""NIC5DigitId"":""14101"","Description"":""Manufacture...""}]"
#  Python's csv.DictReader already unescapes these before we see them,
#  so by the time the value reaches this function it is valid JSON.
# ─────────────────────────────────────────────────────────────────────────────
def _extract_nic_codes(raw_value) -> list[str]:
    """
    Parse all 5-digit NIC codes from the Activities column value.

    Handles:
      - JSON array (current API format):
            [{"NIC5DigitId":"14101","Description":"..."}]
      - Multiple codes in one row:
            [{"NIC5DigitId":"32409","Description":"..."},
             {"NIC5DigitId":"16296","Description":"..."}]
      - Legacy plain-text fallback (kept for safety):
            "1) 14101; 2) 22199"
    """
    if not raw_value or str(raw_value).strip() in ("", "nan", "NA"):
        return []

    text = str(raw_value).strip()

    # ── Primary path: JSON array ──────────────────────────────────────────
    if text.startswith("["):
        try:
            entries = json.loads(text)
            codes = []
            for entry in entries:
                code = str(entry.get("NIC5DigitId", "")).strip()
                if code and code not in ("", "nan"):
                    codes.append(code.zfill(5))
            return codes
        except (json.JSONDecodeError, AttributeError, TypeError) as exc:
            log.debug(f"JSON parse failed for Activities value, falling back: {exc}")
            # fall through to legacy parser

    # ── Fallback: legacy semicolon-separated plain text ───────────────────
    codes = []
    for part in text.split(";"):
        part = part.strip()
        if ")" in part:
            part = part.split(")")[-1].strip()
        if part and part not in ("", "nan"):
            codes.append(part.zfill(5))
    return codes


# ─────────────────────────────────────────────────────────────────────────────
#  PROCESS ONE STATE  (FIX 5: updated column mapping)
# ─────────────────────────────────────────────────────────────────────────────
def process_state(
    state: str,
    nic_set: set[str],
    nic_desc: dict[str, str],
    cat_map: dict[str, str],
) -> list[dict]:
    """
    Fetch all pages for a state using blind CSV pagination — keep fetching
    until an empty page is returned. No JSON probe needed.
    """
    log.info(f"[{state}] Starting blind CSV pagination.")

    all_rows: list[dict] = []
    page_num = 0

    for offset in range(0, 10**9, BATCH_SIZE):
        page_num += 1
        rows = fetch_page_csv(state, offset)

        if not rows:
            log.info(
                f"[{state}] Empty page at offset={offset} — "
                f"pagination complete ({len(all_rows):,} records fetched)."
            )
            break

        # Validate Activities column exists (warn once on first page)
        if page_num == 1 and ACTIVITIES_COLUMN not in rows[0]:
            available = list(rows[0].keys())
            log.error(
                f"[{state}] Expected column '{ACTIVITIES_COLUMN}' not found. "
                f"Available columns: {available}. Skipping state."
            )
            return []

        all_rows.extend(rows)
        log.info(f"[{state}] page {page_num} — {len(all_rows):,} records so far")

    # ── Filter and enrich ──────────────────────────────────────────────────
    # Each row may have multiple NIC codes in Activities.
    # We emit one output row per matching NIC code.
    results: list[dict] = []
    for row in all_rows:
        for code in _extract_nic_codes(row.get(ACTIVITIES_COLUMN, "")):
            if code not in nic_set:
                continue
            results.append({
                "State":              str(row.get("State",                state)).strip().title(),
                "District":           str(row.get("District",                 "")).strip().title(),
                "Pincode":            str(row.get("Pincode",                  "")).strip(),
                "Enterprise_Name":    str(row.get("EnterpriseName",           "")).strip().title(),
                "Registration_Date":  str(row.get("RegistrationDate",         "")).strip(),
                "Address":            str(row.get("CommunicationAddress",     "")).strip().title(),
                "NIC_Code":           code,
                "NIC_Description":    nic_desc.get(code, ""),
                "Category":           cat_map.get(code, "Uncategorised"),
            })

    log.info(
        f"[{state}] {len(results):,} matching supplier rows "
        f"(from {len(all_rows):,} total records)"
    )
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  CHECKPOINT HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _load_checkpoint() -> dict:
    if not os.path.exists(CHECKPOINT):
        return {"completed": [], "failed": []}
    try:
        with open(CHECKPOINT, encoding="utf-8") as f:
            data = json.load(f)
        log.info(
            f"Checkpoint: {len(data.get('completed', []))} completed, "
            f"{len(data.get('failed', []))} previously failed."
        )
        return data
    except Exception as exc:
        log.warning(f"Could not read checkpoint ({exc}). Starting fresh.")
        return {"completed": [], "failed": []}


def _save_checkpoint(cp: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tmp = CHECKPOINT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)
    os.replace(tmp, CHECKPOINT)


# ─────────────────────────────────────────────────────────────────────────────
#  SAVE STATE CSV
# ─────────────────────────────────────────────────────────────────────────────
def _safe_filename(s: str) -> str:
    return (
        s.strip().title()
        .replace(" ", "_").replace("/", "-")
        .replace("\\", "-").replace(":", "")
    )


def save_state_csv(state: str, records: list[dict]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"suppliers_{_safe_filename(state)}.csv")
    pd.DataFrame(records).to_csv(path, index=False, encoding="utf-8-sig")
    log.info(f"[{state}] → saved {len(records):,} rows to {path}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="MSME Supplier Fetcher v3")
    parser.add_argument(
        "--reset", action="store_true",
        help="Ignore checkpoint and restart from first state",
    )
    parser.add_argument(
        "--state", type=str, default=None,
        help="Run a single state only, e.g. --state DELHI",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and filter but do not write any CSV files",
    )
    args = parser.parse_args()

    if not API_KEY:
        log.error("DATA_GOV_API_KEY is not set. Aborting.")
        sys.exit(1)

    nic_set, nic_desc = load_nic_codes()
    cat_map           = load_category_mapping()

    if args.state:
        target = args.state.strip().upper()
        if target not in STATES_AND_UTS:
            log.error(
                f"Unknown state: {target!r}.\nValid values:\n  "
                + "\n  ".join(STATES_AND_UTS)
            )
            sys.exit(1)
        pending   = [target]
        completed: set[str] = set()
        failed:    list[str] = []
    else:
        cp        = {} if args.reset else _load_checkpoint()
        completed = set(cp.get("completed", []))
        failed    = list(cp.get("failed",    []))
        pending   = [s for s in STATES_AND_UTS if s not in completed]

    skipped = len(STATES_AND_UTS) - len(pending) if not args.state else 0

    print(f"\n{'═'*64}")
    print(f"  MSME Supplier Fetcher  v3  {'[DRY RUN]' if args.dry_run else ''}")
    print(f"  Resource ID      : {RESOURCE_ID}")
    print(f"  NIC codes loaded : {len(nic_set)}")
    print(f"  States pending   : {len(pending)}  (skipped: {skipped})")
    print(f"  Batch size       : {BATCH_SIZE} records/page")
    print(f"  Request gap      : {MIN_REQUEST_GAP}s → ~{int(3600/MIN_REQUEST_GAP)} req/hr (limit 1,000)")
    print(f"  Output folder    : {OUTPUT_DIR}/suppliers_<State>.csv")
    print(f"{'═'*64}\n")

    total_suppliers = 0

    for i, state in enumerate(pending, 1):
        log.info(f"[{i:02d}/{len(pending)}] ── {state.title()} ──")
        try:
            records = process_state(state, nic_set, nic_desc, cat_map)

            if not args.dry_run:
                if records:
                    save_state_csv(state, records)
                else:
                    log.info(f"[{state}] No matching suppliers — no file written.")

            total_suppliers += len(records)
            completed.add(state)
            failed = [s for s in failed if s != state]

        except Exception as exc:
            log.error(f"[{state}] FAILED: {exc}")
            if state not in failed:
                failed.append(state)

        if not args.state:
            _save_checkpoint({"completed": sorted(completed), "failed": failed})

    still_pending = [s for s in STATES_AND_UTS if s not in completed]

    print(f"\n{'═'*64}")
    print(f"  Total supplier rows saved : {total_suppliers:,}")
    print(f"  Output folder             : {OUTPUT_DIR}/")
    if failed:
        print(f"\n  States FAILED (retried automatically on next run):")
        for s in failed:
            print(f"    ✗  {s.title()}")
    if still_pending:
        print(f"\n  States still pending (not reached in this run):")
        for s in still_pending:
            print(f"    ○  {s.title()}")
    if not failed and not still_pending:
        print("  ✓  All states completed successfully.")
    print(f"{'═'*64}\n")

    if still_pending or failed:
        sys.exit(2)
    else:
        if not args.state and not args.dry_run and os.path.exists(CHECKPOINT):
            os.remove(CHECKPOINT)
            log.info("Checkpoint cleared — clean full run complete.")
        sys.exit(0)


if __name__ == "__main__":
    main()


# ─────────────────────────────────────────────────────────────────────────────
#  TIMING REFERENCE  (1000 records/page, 4.5s gap → 800 req/hr)
# ─────────────────────────────────────────────────────────────────────────────
# State                    Est. records   Pages    Time
# ──────────────────────── ────────────── ──────   ──────
# Uttar Pradesh                  950,000     951   ~1.19 hrs
# Maharashtra                    700,000     700   ~0.88 hrs
# Gujarat                        650,000     650   ~0.81 hrs
# Rajasthan                      500,000     500   ~0.63 hrs
# Tamil Nadu                     480,000     480   ~0.60 hrs
# West Bengal                    420,000     420   ~0.53 hrs
# Madhya Pradesh                 380,000     380   ~0.48 hrs
# Karnataka                      360,000     360   ~0.45 hrs
# Andhra Pradesh                 310,000     310   ~0.39 hrs
# Bihar                          300,000     300   ~0.38 hrs
# ... (26 smaller states)      1,697,500   1,698   ~2.12 hrs
# ──────────────────────── ────────────── ──────   ──────
# TOTAL                        7,247,500   7,249   ~9.1 hrs
# ─────────────────────────────────────────────────────────────────────────────
