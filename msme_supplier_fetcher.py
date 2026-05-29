"""
MSME Supplier Fetcher
=====================
Fetches MSME registered units from data.gov.in, filters by NIC codes
defined in data/Key_NIC_Codes_List.xlsx, maps categories from
data/Demand_Excel_Filled.xlsx, and writes one
    data/suppliers/suppliers_<State>.csv
per state.

Rate limit: data.gov.in allows 1,000 requests/hour (rolling).
This script enforces a minimum 4.0s gap between every API call,
targeting ~900 req/hr to leave a safety margin.

Checkpoint: completed states are saved to data/suppliers/fetch_checkpoint.json
after each state so runs can resume after a timeout or failure.

Exit codes (read by the GitHub Actions workflow):
    0  — all states completed cleanly
    2  — run ended with states still pending (timeout / partial run);
         the workflow uses this to auto-retrigger itself

Requirements:
    pip install requests pandas openpyxl

Usage:
    python msme_supplier_fetcher.py               # normal run / resume
    python msme_supplier_fetcher.py --reset       # ignore checkpoint, start fresh
    python msme_supplier_fetcher.py --state DELHI # run a single state
    python msme_supplier_fetcher.py --dry-run     # fetch + filter but don't write CSVs
"""

import argparse
import json
import logging
import os
import sys
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG  — edit only this block
# ─────────────────────────────────────────────────────────────────────────────
# API key is read from the DATA_GOV_API_KEY environment variable (set in GitHub
# Actions secrets).  The fallback hardcoded value is used for local runs only.
API_KEY = os.environ.get(
    "DATA_GOV_API_KEY",
    "579b464db66ec23bdd000001d2ecc2400ab74128657eb9c1309228b3",
)
RESOURCE_ID = "8b68ae56-84cf-4728-a0a6-1be11028dea7"
BASE_URL    = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

NIC_CODES_FILE = "data/Key_NIC_Codes_List.xlsx"
DEMAND_FILE    = "data/Demand_Excel_Filled.xlsx"
OUTPUT_DIR     = "data/suppliers"
CHECKPOINT     = os.path.join(OUTPUT_DIR, "fetch_checkpoint.json")

BATCH_SIZE     = 1000   # records per API page; max the API supports
TIMEOUT_SEC    = 90     # per-request timeout
MAX_RETRIES    = 4      # application-level retries (on top of urllib3 retries)
RETRY_BASE_SEC = 5      # first retry wait; doubles each attempt → 5,10,20,40s

# ── Rate limiter ──────────────────────────────────────────────────────────────
# Hard limit is 1,000 req/hr rolling.  We target ~900/hr → 4.0s between calls.
# _last_request_at tracks the wall-clock time of the last call so any time
# already spent in JSON parsing / file I/O counts toward the gap.
MIN_REQUEST_GAP  = 4.0   # seconds; raise to 4.5 if you still see 429s
_last_request_at: float = 0.0

# ─────────────────────────────────────────────────────────────────────────────
#  STATES / UTs
# ─────────────────────────────────────────────────────────────────────────────
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
#  urllib3 retries handle transient TCP/TLS errors.
#  Application-level retries in fetch_page() handle 502s and empty bodies.
# ─────────────────────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

SESSION = _build_session()

# ─────────────────────────────────────────────────────────────────────────────
#  RATE-LIMITED FETCH
# ─────────────────────────────────────────────────────────────────────────────
def _throttle():
    """Sleep just long enough so we never fire faster than MIN_REQUEST_GAP."""
    global _last_request_at
    elapsed = time.monotonic() - _last_request_at
    gap = MIN_REQUEST_GAP - elapsed
    if gap > 0:
        time.sleep(gap)
    _last_request_at = time.monotonic()


def fetch_page(state: str, offset: int) -> dict:
    """
    Fetch one page of results for a state.
    Returns the parsed JSON dict.
    Raises RuntimeError after MAX_RETRIES consecutive failures.
    """
    params = {
        "api-key":        API_KEY,
        "format":         "json",
        "limit":          BATCH_SIZE,
        "offset":         offset,
        "filters[State]": state,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        _throttle()
        try:
            resp = SESSION.get(BASE_URL, params=params, timeout=TIMEOUT_SEC)

            # 429: rate limited — back off hard before retrying
            if resp.status_code == 429:
                wait = 60 * attempt   # 60s, 120s, 180s, 240s
                log.warning(
                    f"[{state}] offset={offset} — 429 Rate Limited. "
                    f"Backing off {wait}s (attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            # API sometimes returns HTTP 200 with an error body and no 'records'
            if "records" not in data:
                if attempt < MAX_RETRIES:
                    wait = RETRY_BASE_SEC * (2 ** (attempt - 1))
                    log.warning(
                        f"[{state}] offset={offset} — no 'records' in response "
                        f"(attempt {attempt}/{MAX_RETRIES}). "
                        f"Body: {str(data)[:200]}. Retrying in {wait}s"
                    )
                    time.sleep(wait)
                    continue
                # Last attempt: return what we have so the caller can decide
                return data

            return data

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as exc:
            wait = RETRY_BASE_SEC * (2 ** (attempt - 1))   # 5, 10, 20, 40s
            log.warning(
                f"[{state}] offset={offset} attempt {attempt}/{MAX_RETRIES}: "
                f"{exc}. Retrying in {wait}s"
            )
            time.sleep(wait)

    raise RuntimeError(
        f"[{state}] offset={offset} failed after {MAX_RETRIES} attempts."
    )

# ─────────────────────────────────────────────────────────────────────────────
#  LOAD REFERENCE FILES
# ─────────────────────────────────────────────────────────────────────────────
def load_nic_codes() -> tuple[set, dict]:
    """
    Reads Key_NIC_Codes_List.xlsx.
    Finds the NIC code column (must contain 'nic') and description column
    (must contain 'desc').
    Returns (nic_set, nic_description_map).
    """
    df = pd.read_excel(NIC_CODES_FILE, dtype=str)
    df.columns = df.columns.str.strip()

    # Prefer a column that has both 'nic' and 'code'; fall back to just 'nic'
    code_col = next(
        (c for c in df.columns if "nic" in c.lower() and "code" in c.lower()),
        None,
    ) or next(
        (c for c in df.columns if "nic" in c.lower()),
        None,
    )
    if code_col is None:
        raise ValueError(
            f"Cannot find a NIC code column in {NIC_CODES_FILE}. "
            f"Columns present: {list(df.columns)}"
        )

    desc_col = next(
        (c for c in df.columns if "desc" in c.lower()),
        None,
    )

    df[code_col] = df[code_col].str.strip().str.zfill(5)
    nic_set  = set(df[code_col].dropna().tolist())
    nic_desc = (
        dict(zip(df[code_col], df[desc_col].fillna(""))) if desc_col else {}
    )

    log.info(
        f"Loaded {len(nic_set)} NIC codes from {NIC_CODES_FILE}  "
        f"(col: {code_col!r})"
    )
    return nic_set, nic_desc


def load_category_mapping() -> dict:
    """
    Reads Demand_Excel_Filled.xlsx.
    Finds the NIC column (contains 'nic') and Category column (contains 'cat').
    Returns {nic_code: category_string}.
    """
    df = pd.read_excel(DEMAND_FILE, dtype=str)
    df.columns = df.columns.str.strip()

    nic_col = next((c for c in df.columns if "nic" in c.lower()), None)
    cat_col = next((c for c in df.columns if "cat" in c.lower()), None)

    if nic_col is None or cat_col is None:
        raise ValueError(
            f"Cannot find NIC or Category columns in {DEMAND_FILE}. "
            f"Columns present: {list(df.columns)}"
        )

    df[nic_col] = df[nic_col].str.strip().str.zfill(5)
    mapping = (
        df.dropna(subset=[nic_col, cat_col])
        .groupby(nic_col)[cat_col]
        .agg(lambda x: x.mode().iloc[0])
        .to_dict()
    )

    log.info(
        f"Loaded {len(mapping)} NIC→Category mappings from {DEMAND_FILE}  "
        f"(cols: {nic_col!r}, {cat_col!r})"
    )
    return mapping

# ─────────────────────────────────────────────────────────────────────────────
#  NIC CODE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
def _find_nic_column(columns) -> str | None:
    """
    Locate the NIC-code column from whatever the API returns.
    Logs all column names so you can update keywords if the API changes.
    """
    log.info(f"  API columns seen: {list(columns)}")
    for kw in ["nic5digit", "nic5", "niccode", "nic_code", "nic"]:
        for col in columns:
            if kw in col.lower():
                log.info(f"  → NIC column matched: {col!r}  (keyword: {kw!r})")
                return col
    return None


def _extract_nic_codes(raw_value) -> list[str]:
    """
    Parse all 5-digit NIC codes from a raw cell value.
    Handles: "14101", "14101; 22199", "1) 14101; 2) 22199; 3) 32909"
    Returns zero-padded 5-character strings.
    """
    if not raw_value or str(raw_value).strip() in ("", "nan", "NA"):
        return []
    codes = []
    for part in str(raw_value).split(";"):
        part = part.strip()
        if ")" in part:
            part = part.split(")")[-1].strip()
        if part and part != "nan":
            codes.append(part.zfill(5))
    return codes

# ─────────────────────────────────────────────────────────────────────────────
#  PROCESS ONE STATE
# ─────────────────────────────────────────────────────────────────────────────
def process_state(
    state: str,
    nic_set: set,
    nic_desc: dict,
    cat_map: dict,
) -> list[dict]:
    """
    Fetches all pages for a state, filters to matching NIC codes, and
    returns a list of result-row dicts.
    """
    first      = fetch_page(state, 0)
    total      = int(first.get("total", 0))

    if total == 0:
        log.info(f"[{state}] No records in API — skipping.")
        return []

    all_records = list(first.get("records", []))
    offsets     = list(range(BATCH_SIZE, total, BATCH_SIZE))
    total_pages = 1 + len(offsets)

    log.info(f"[{state}] {total:,} records across {total_pages} page(s)")

    for page_num, offset in enumerate(offsets, start=2):
        data = fetch_page(state, offset)
        all_records.extend(data.get("records", []))
        log.info(
            f"[{state}] page {page_num}/{total_pages}  "
            f"({len(all_records):,}/{total:,} fetched)"
        )

    df = pd.DataFrame(all_records)
    if df.empty:
        log.warning(f"[{state}] DataFrame empty after assembling all pages.")
        return []

    nic_col = _find_nic_column(df.columns)
    if nic_col is None:
        log.error(
            f"[{state}] Could not find a NIC code column. "
            f"Update _find_nic_column() keywords to match: {list(df.columns)}"
        )
        return []

    results: list[dict] = []
    for _, row in df.iterrows():
        for code in _extract_nic_codes(row.get(nic_col, "")):
            if code not in nic_set:
                continue
            results.append({
                "State":           str(row.get("State",          state)).strip().title(),
                "District":        str(row.get("District",       "")).strip().title(),
                "Pincode":         str(row.get("Pincode",        "")).strip(),
                "Enterprise_Name": str(row.get("EnterpriseName", "")).strip().title(),
                "NIC_Code":        code,
                "NIC_Description": nic_desc.get(code, ""),
                "Category":        cat_map.get(code, "Uncategorised"),
                "Enterprise_Type": str(row.get("EnterpriseType", "")).strip().title(),
                "Major_Activity":  str(row.get("MajorActivity",  "")).strip().title(),
            })

    log.info(
        f"[{state}] {len(results):,} matching supplier rows "
        f"(from {len(df):,} total records)"
    )
    return results

# ─────────────────────────────────────────────────────────────────────────────
#  CHECKPOINT
# ─────────────────────────────────────────────────────────────────────────────
def _load_checkpoint() -> dict:
    if not os.path.exists(CHECKPOINT):
        return {"completed": [], "failed": []}
    try:
        with open(CHECKPOINT, encoding="utf-8") as f:
            data = json.load(f)
        log.info(
            f"Checkpoint found — "
            f"{len(data.get('completed', []))} completed, "
            f"{len(data.get('failed', []))} previously failed."
        )
        return data
    except Exception as exc:
        log.warning(f"Could not read checkpoint ({exc}). Starting fresh.")
        return {"completed": [], "failed": []}


def _save_checkpoint(cp: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tmp = CHECKPOINT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)
    os.replace(tmp, CHECKPOINT)   # atomic on POSIX

# ─────────────────────────────────────────────────────────────────────────────
#  SAVE
# ─────────────────────────────────────────────────────────────────────────────
def _safe_filename(s: str) -> str:
    return (
        s.strip().title()
        .replace(" ", "_").replace("/", "-")
        .replace("\\", "-").replace(":", "")
    )


def save_state_csv(state: str, records: list[dict]):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"suppliers_{_safe_filename(state)}.csv")
    pd.DataFrame(records).to_csv(path, index=False, encoding="utf-8-sig")
    log.info(f"[{state}] → saved {len(records):,} rows to {path}")

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="MSME Supplier Fetcher")
    parser.add_argument(
        "--reset", action="store_true",
        help="Ignore checkpoint and restart from the first state",
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

    # ── Reference data ────────────────────────────────────────────────────────
    nic_set, nic_desc = load_nic_codes()
    cat_map           = load_category_mapping()

    # ── Build work list ───────────────────────────────────────────────────────
    if args.state:
        target = args.state.strip().upper()
        if target not in STATES_AND_UTS:
            log.error(
                f"Unknown state: {target!r}. "
                f"Valid values:\n  " + "\n  ".join(STATES_AND_UTS)
            )
            sys.exit(1)
        pending   = [target]
        completed = set()
        failed    = []
    else:
        cp        = {} if args.reset else _load_checkpoint()
        completed = set(cp.get("completed", []))
        failed    = list(cp.get("failed",    []))
        pending   = [s for s in STATES_AND_UTS if s not in completed]

    skipped = len(STATES_AND_UTS) - len(pending) if not args.state else 0

    print(f"\n{'='*62}")
    print(f"  MSME Supplier Fetcher  {'[DRY RUN]' if args.dry_run else ''}")
    print(f"  NIC codes loaded    : {len(nic_set)}")
    print(f"  States pending      : {len(pending)}")
    if skipped:
        print(f"  States skipped      : {skipped}  (already completed)")
    print(f"  Batch size          : {BATCH_SIZE} records/page")
    print(f"  Request gap         : {MIN_REQUEST_GAP}s  (~{int(3600/MIN_REQUEST_GAP)}/hr)")
    print(f"  Output              : {OUTPUT_DIR}/suppliers_<State>.csv")
    print(f"{'='*62}\n")

    # ── Process states sequentially ───────────────────────────────────────────
    total_suppliers = 0

    for i, state in enumerate(pending, 1):
        log.info(f"[{i:02d}/{len(pending)}] Starting: {state.title()}")
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

        # Checkpoint after every state so partial runs are always resumable
        if not args.state:
            _save_checkpoint({"completed": sorted(completed), "failed": failed})

    # ── Summary ───────────────────────────────────────────────────────────────
    still_pending = [s for s in STATES_AND_UTS if s not in completed]

    print(f"\n{'='*62}")
    print(f"  Total supplier rows saved : {total_suppliers:,}")
    print(f"  Output folder             : {OUTPUT_DIR}/")
    if failed:
        print(f"\n  States that FAILED (will be retried on next run):")
        for s in failed:
            print(f"    - {s.title()}")
    if still_pending:
        print(f"\n  States still pending (not yet reached in this run):")
        for s in still_pending:
            print(f"    - {s.title()}")
    if not failed and not still_pending:
        print(f"  All states completed successfully.")
    print(f"{'='*62}\n")

    # ── Exit codes ────────────────────────────────────────────────────────────
    # Exit 2 signals the GitHub Actions workflow to retrigger itself.
    # Exit 1 is reserved for hard errors (bad API key, missing files, etc.).
    if still_pending or failed:
        sys.exit(2)   # incomplete — workflow will retrigger
    else:
        # Clean run: remove checkpoint so next scheduled run starts fresh
        if not args.state and not args.dry_run and os.path.exists(CHECKPOINT):
            os.remove(CHECKPOINT)
            log.info("Checkpoint cleared — clean full run complete.")
        sys.exit(0)


if __name__ == "__main__":
    main()
