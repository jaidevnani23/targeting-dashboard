"""
MSME Supplier Fetcher  —  Production v2
========================================
Fetches MSME registered units from data.gov.in, filters by NIC codes
defined in data/Key_NIC_Codes_List.xlsx, maps categories from
data/Demand_Excel_Filled.xlsx, and writes one
    data/suppliers/suppliers_<State>.csv
per state.

Rate limit: data.gov.in allows 1,000 requests/hour (rolling window).
This script enforces a 6.0s minimum gap → ~600 req/hr, giving 40%
headroom. The entire run (~7,300 requests across 36 states/UTs) takes
approximately 12 hours. Use the built-in checkpoint to split across
multiple sessions or GitHub Actions runs.

Exit codes (read by GitHub Actions):
    0  — all states completed cleanly
    2  — states still pending (timeout/partial); workflow auto-retriggers

FIXES vs v1
-----------
FIX 1 — urllib3 Retry must NOT include 429 in status_forcelist.
    urllib3 intercepts 429 before application code sees it, consuming
    retries with tiny backoffs, then raises MaxRetryError. The
    application-layer 429 handler (with long waits) never fires.
    → 429 removed from status_forcelist. Handled only in app layer.

FIX 2 — fetch_total fallback when total == 0.
    Some data.gov.in resources return total=0 in the JSON envelope even
    when records exist (e.g. very large states). Added fallback: if
    total==0, attempt one CSV page; if rows return, switch to blind
    pagination (stop only on empty page).

FIX 3 — NIC column detection broadened.
    API response column is "NICCode5Digit". Multi-priority scan added
    so it matches this and any future renames.

FIX 4 — 429 backoff increased.
    Old: 60 * attempt (60, 120, 180, 240s).
    New: 120 + 60 * attempt (180, 240, 300, 360s).
    At 600 req/hr, a 180s wait frees ~50 rolling-window slots —
    enough to resume safely on the first retry in most cases.

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
import math
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG  — edit only this block (or use env vars)
# ─────────────────────────────────────────────────────────────────────────────
API_KEY = os.environ.get(
    "DATA_GOV_API_KEY",
    "579b464db66ec23bdd000001d2ecc2400ab74128657eb9c1309228b3",
)
RESOURCE_ID = os.environ.get(
    "DATA_GOV_RESOURCE_ID",
    "8b68ae56-84cf-4728-a0a6-1be11028dea7",
)
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

NIC_CODES_FILE = "data/Key_NIC_Codes_List.xlsx"
DEMAND_FILE    = "data/Demand_Excel_Filled.xlsx"
OUTPUT_DIR     = "data/suppliers"
CHECKPOINT     = os.path.join(OUTPUT_DIR, "fetch_checkpoint.json")

BATCH_SIZE    = 1000   # records per API page
TIMEOUT_SEC   = 90     # per-request socket timeout
MAX_RETRIES   = 4      # application-level retry attempts
RETRY_BASE    = 5      # urllib3 backoff base (seconds); 5, 10, 20s

# ── Rate limiter ──────────────────────────────────────────────────────────────
# data.gov.in: 1,000 requests/hour rolling window.
# 6.0s gap → 600 req/hr → 40% headroom.
# Total run: ~7,300 requests × 6s ≈ 12 hours (see timing table at bottom).
MIN_REQUEST_GAP: float = 6.0
_last_request_at: float = 0.0

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
#  FIX 1: 429 is NOT in status_forcelist. It is handled only in app layer
#  with long waits. Including 429 in urllib3's list would intercept it
#  before application code runs, exhaust retries with tiny backoffs, and
#  raise MaxRetryError — bypassing the proper long-wait handler entirely.
# ─────────────────────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,               # waits: 2, 4, 8 seconds
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
    """Block until at least MIN_REQUEST_GAP seconds have passed since the
    last call. Guarantees we never exceed 600 req/hr."""
    global _last_request_at
    elapsed = time.monotonic() - _last_request_at
    gap = MIN_REQUEST_GAP - elapsed
    if gap > 0:
        time.sleep(gap)
    _last_request_at = time.monotonic()

# ─────────────────────────────────────────────────────────────────────────────
#  FETCH TOTAL  (JSON probe — 1 call per state)
#  FIX 2: If total == 0, we fall back to blind CSV pagination.
#         Some large states return total=0 in the envelope incorrectly.
# ─────────────────────────────────────────────────────────────────────────────
def fetch_total(state: str) -> int:
    """
    Returns the API-reported total record count for a state.
    Returns -1 if the API returns 0 (triggers fallback pagination).
    Returns 0 only after all retries fail (skip the state).
    """
    params = {
        "api-key":        API_KEY,
        "format":         "json",
        "limit":          1,
        "offset":         0,
        "filters[State]": state,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        _throttle()
        try:
            resp = SESSION.get(BASE_URL, params=params, timeout=TIMEOUT_SEC)

            if resp.status_code == 429:
                # FIX 4: long backoff on 429
                wait = 120 + (60 * attempt)
                log.warning(
                    f"[{state}] probe 429 — backing off {wait}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data  = resp.json()
            total = int(data.get("total", 0))

            if total == 0:
                # FIX 2: could be an API envelope bug; use -1 as sentinel
                log.warning(
                    f"[{state}] JSON probe returned total=0. "
                    "Will attempt blind CSV pagination as fallback."
                )
                return -1

            log.info(f"[{state}] API total: {total:,} records "
                     f"(~{math.ceil(total/BATCH_SIZE)} pages)")
            return total

        except Exception as exc:
            wait = RETRY_BASE * (2 ** (attempt - 1))
            log.warning(
                f"[{state}] probe attempt {attempt}/{MAX_RETRIES}: "
                f"{exc}. Retrying in {wait}s"
            )
            time.sleep(wait)

    log.error(f"[{state}] Could not retrieve total after {MAX_RETRIES} attempts.")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
#  FETCH ONE CSV PAGE
# ─────────────────────────────────────────────────────────────────────────────
def fetch_page_csv(state: str, offset: int) -> list[dict]:
    """
    Fetch one batch of records as CSV. Returns [] on empty page (end of data).
    Raises RuntimeError after MAX_RETRIES failures (caller marks state failed).

    FIX 4: 429 backoff is 120 + 60*attempt seconds (180, 240, 300, 360s).
    At 600 req/hr, a 180s wait frees ~50 rolling-window slots, which is
    sufficient to safely resume on the next attempt.

    FIX 1: 429 is handled here, not in urllib3, so we get the full wait time.
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
            resp = SESSION.get(BASE_URL, params=params, timeout=TIMEOUT_SEC)

            if resp.status_code == 429:
                wait = 120 + (60 * attempt)   # FIX 4: 180, 240, 300, 360s
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
                # Empty body = past last page
                log.debug(f"[{state}] offset={offset} — empty body, end of data.")
                return []

            reader = csv.DictReader(io.StringIO(text))
            rows   = list(reader)

            if not rows:
                log.debug(f"[{state}] offset={offset} — header-only CSV, end of data.")
                return []

            # Log column names once on the first page (aids debugging FIX 3)
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
#  NIC COLUMN DETECTION  (FIX 3)
# ─────────────────────────────────────────────────────────────────────────────
def _find_nic_column(columns) -> Optional[str]:
    """
    Multi-priority scan for the NIC code column.
    API currently returns 'NICCode5Digit'. Handles renames gracefully.

    Priority 1: name contains both 'nic' and '5'  → "NICCode5Digit"
    Priority 2: name contains both 'nic' and 'digit'
    Priority 3: legacy keywords: 'niccode', 'nic_code', 'nic'
    """
    cols_lower = {c.lower(): c for c in columns}

    for lo, orig in cols_lower.items():
        if "nic" in lo and "5" in lo:
            log.info(f"  NIC column (priority 1): {orig!r}")
            return orig

    for lo, orig in cols_lower.items():
        if "nic" in lo and "digit" in lo:
            log.info(f"  NIC column (priority 2): {orig!r}")
            return orig

    for kw in ("niccode", "nic_code", "nic"):
        for lo, orig in cols_lower.items():
            if kw in lo:
                log.info(f"  NIC column (priority 3 / kw={kw!r}): {orig!r}")
                return orig

    log.error(
        f"  Could not find NIC column. "
        f"Add a keyword to _find_nic_column() for: {list(columns)}"
    )
    return None


def _extract_nic_codes(raw_value) -> list[str]:
    """
    Parse all 5-digit NIC codes from a cell value.
    Handles: "14101", "14101; 22199", "1) 14101; 2) 22199; 3) 32909"
    """
    if not raw_value or str(raw_value).strip() in ("", "nan", "NA"):
        return []
    codes = []
    for part in str(raw_value).split(";"):
        part = part.strip()
        if ")" in part:
            part = part.split(")")[-1].strip()
        if part and part not in ("", "nan"):
            codes.append(part.zfill(5))
    return codes


# ─────────────────────────────────────────────────────────────────────────────
#  PROCESS ONE STATE
# ─────────────────────────────────────────────────────────────────────────────
def process_state(
    state: str,
    nic_set: set[str],
    nic_desc: dict[str, str],
    cat_map: dict[str, str],
) -> list[dict]:
    """
    Fetch all pages for a state, filter by NIC codes, return result rows.

    FIX 2: If fetch_total returns -1 (total=0 from API despite data
    existing), we use blind pagination: keep fetching until an empty page.
    """
    total    = fetch_total(state)
    blind    = (total == -1)   # FIX 2: fallback mode flag

    if total == 0:
        log.info(f"[{state}] No records — skipping.")
        return []

    if blind:
        log.info(f"[{state}] Using blind pagination (total unknown).")
    else:
        expected_pages = math.ceil(total / BATCH_SIZE)
        log.info(f"[{state}] {total:,} records → ~{expected_pages} pages")

    all_rows: list[dict] = []
    nic_col:  Optional[str] = None
    page_num  = 0

    for offset in range(0, (total if not blind else 10**9), BATCH_SIZE):
        page_num += 1
        rows = fetch_page_csv(state, offset)

        if not rows:
            log.info(
                f"[{state}] Empty page at offset={offset} — "
                f"pagination complete ({len(all_rows):,} records fetched)."
            )
            break

        # Discover NIC column name from the first non-empty page
        if nic_col is None:
            nic_col = _find_nic_column(rows[0].keys())
            if nic_col is None:
                log.error(
                    f"[{state}] Cannot find NIC column. "
                    f"Skipping state. Columns: {list(rows[0].keys())}"
                )
                return []

        all_rows.extend(rows)

        if not blind:
            log.info(
                f"[{state}] page {page_num}/{expected_pages}  "
                f"({len(all_rows):,}/{total:,})"
            )
        else:
            log.info(f"[{state}] page {page_num} — {len(all_rows):,} records so far")

    # ── Filter and enrich ──────────────────────────────────────────────────
    results: list[dict] = []
    for row in all_rows:
        for code in _extract_nic_codes(row.get(nic_col, "")):
            if code not in nic_set:
                continue
            results.append({
                "State":           str(row.get("State",          state)).strip().title(),
                "District":        str(row.get("District",           "")).strip().title(),
                "Pincode":         str(row.get("Pincode",             "")).strip(),
                "Enterprise_Name": str(row.get("EnterpriseName",      "")).strip().title(),
                "NIC_Code":        code,
                "NIC_Description": nic_desc.get(code, ""),
                "Category":        cat_map.get(code, "Uncategorised"),
                "Enterprise_Type": str(row.get("EnterpriseType",      "")).strip().title(),
                "Major_Activity":  str(row.get("MajorActivity",       "")).strip().title(),
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
    os.replace(tmp, CHECKPOINT)   # atomic write


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
    parser = argparse.ArgumentParser(description="MSME Supplier Fetcher v2")
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

    # ── Determine which states to process ─────────────────────────────────
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

    # ── Banner ────────────────────────────────────────────────────────────
    est_reqs     = sum(
        math.ceil(1_000) + 1 for _ in pending   # conservative 1 page min + probe
    )
    est_hrs_low  = (len(pending) * 2  * MIN_REQUEST_GAP) / 3600   # 2 req/state min
    est_hrs_full = 12.1  # pre-calculated for full 36-state run

    print(f"\n{'═'*64}")
    print(f"  MSME Supplier Fetcher  v2  {'[DRY RUN]' if args.dry_run else ''}")
    print(f"  Resource ID      : {RESOURCE_ID}")
    print(f"  NIC codes loaded : {len(nic_set)}")
    print(f"  States pending   : {len(pending)}  (skipped: {skipped})")
    print(f"  Request gap      : {MIN_REQUEST_GAP}s → ~{int(3600/MIN_REQUEST_GAP)} req/hr (limit 1,000)")
    print(f"  Est. total time  : ~{est_hrs_full:.1f} hrs for a full 36-state run")
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

        # Save checkpoint after every state (atomic write)
        if not args.state:
            _save_checkpoint({"completed": sorted(completed), "failed": failed})

    # ── Summary ───────────────────────────────────────────────────────────
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

    # Exit 2 triggers GitHub Actions auto-retrigger
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
#  TIMING REFERENCE TABLE (pre-calculated, 6.0s gap)
# ─────────────────────────────────────────────────────────────────────────────
# State                                        Est. records   Reqs    Time
# ─────────────────────────────────────────── ────────────── ──────  ──────
# Uttar Pradesh                                    950,000      951   ~95 min
# Maharashtra                                      700,000      701   ~70 min
# Gujarat                                          650,000      651   ~65 min
# Rajasthan                                        500,000      501   ~50 min
# Tamil Nadu                                       480,000      481   ~48 min
# West Bengal                                      420,000      421   ~42 min
# Madhya Pradesh                                   380,000      381   ~38 min
# Karnataka                                        360,000      361   ~36 min
# Andhra Pradesh                                   310,000      311   ~31 min
# Bihar                                            300,000      301   ~30 min
# ... (26 smaller states)                      ~1,697,500    1,743   ~174 min
# ─────────────────────────────────────────── ────────────── ──────  ──────
# TOTAL                                          7,247,500    7,284   ~728 min (~12.1 hrs)
# ─────────────────────────────────────────────────────────────────────────────
