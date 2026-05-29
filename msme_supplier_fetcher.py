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

CHANGES vs original
-------------------
FIX 1 — CSV response format
    The API returns CSV (not JSON) for this resource even when format=json is
    passed. Requesting format=csv and parsing with csv.DictReader is reliable
    and much faster than trying to JSON-decode a mixed CSV+JSON body.

FIX 2 — NIC column name
    The API response contains a column named "NICCode5Digit" (mixed case, no
    separator). The original keyword list missed this. A broader scan now
    catches it as well as any future renames.

FIX 3 — total record count
    The CSV response has no "total" envelope field. We now fetch a 1-record
    probe page first (format=json, limit=1) to read the total count, then
    stream all pages as CSV. This avoids re-parsing JSON for large payloads.

FIX 4 — empty-page sentinel
    An empty CSV body (no data rows) is treated as end-of-results rather
    than a retryable error, preventing infinite retry loops on the last page.

FIX 5 — resource ID note
    The resource ID visible in the API explorer URL differs slightly from the
    one hard-coded in the original script. Verify the correct ID and set it
    via RESOURCE_ID below or the DATA_GOV_RESOURCE_ID env var.
"""

import argparse
import csv
import io
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
API_KEY = os.environ.get(
    "DATA_GOV_API_KEY",
    "579b464db66ec23bdd000001d2ecc2400ab74128657eb9c1309228b3",
)

# FIX 5: The explorer URL shows a slightly different resource ID ending in
# "dea7ap1" vs the original "dea7". Verify which is correct in your account
# and set DATA_GOV_RESOURCE_ID in your environment / GitHub secret if needed.
RESOURCE_ID = os.environ.get(
    "DATA_GOV_RESOURCE_ID",
    "8b68ae56-84cf-4728-a0a6-1be11028dea7",   # ← confirm this matches the portal
)
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

NIC_CODES_FILE = "data/Key_NIC_Codes_List.xlsx"
DEMAND_FILE    = "data/Demand_Excel_Filled.xlsx"
OUTPUT_DIR     = "data/suppliers"
CHECKPOINT     = os.path.join(OUTPUT_DIR, "fetch_checkpoint.json")

BATCH_SIZE     = 1000   # records per API page; max the API supports
TIMEOUT_SEC    = 90     # per-request timeout
MAX_RETRIES    = 4      # application-level retries
RETRY_BASE_SEC = 5      # first retry wait; doubles → 5, 10, 20, 40s

# Rate limiter: 1,000 req/hr hard limit → target ~900/hr → 4.0s between calls
MIN_REQUEST_GAP  = 4.0
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
    global _last_request_at
    elapsed = time.monotonic() - _last_request_at
    gap = MIN_REQUEST_GAP - elapsed
    if gap > 0:
        time.sleep(gap)
    _last_request_at = time.monotonic()


def _base_params(state: str, offset: int, fmt: str) -> dict:
    return {
        "api-key":        API_KEY,
        "format":         fmt,
        "limit":          BATCH_SIZE,
        "offset":         offset,
        "filters[State]": state,
    }


def fetch_total(state: str) -> int:
    """
    FIX 3: Use a tiny JSON probe (limit=1) to read the total record count
    from the envelope. This is the only JSON call per state; all data pages
    are fetched as CSV.
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
                wait = 60 * attempt
                log.warning(f"[{state}] probe 429 — backing off {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            total = int(data.get("total", 0))
            log.info(f"[{state}] Total records reported by API: {total:,}")
            return total
        except Exception as exc:
            wait = RETRY_BASE_SEC * (2 ** (attempt - 1))
            log.warning(f"[{state}] probe attempt {attempt}/{MAX_RETRIES}: {exc}. Retrying in {wait}s")
            time.sleep(wait)
    log.error(f"[{state}] Could not retrieve total count after {MAX_RETRIES} attempts.")
    return 0


def fetch_page_csv(state: str, offset: int) -> list[dict]:
    """
    FIX 1: Fetch one page as CSV and return a list of row dicts.
    CSV parsing is unambiguous and never fails on "mixed" bodies.

    FIX 4: An empty body (no data rows) returns [] so the caller
    stops paginating instead of retrying forever.
    """
    params = _base_params(state, offset, fmt="csv")

    for attempt in range(1, MAX_RETRIES + 1):
        _throttle()
        try:
            resp = SESSION.get(BASE_URL, params=params, timeout=TIMEOUT_SEC)

            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(
                    f"[{state}] offset={offset} 429 — backing off {wait}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            text = resp.text.strip()

            if not text:
                # FIX 4: empty body → end of results, not an error
                log.debug(f"[{state}] offset={offset} — empty body, stopping pagination.")
                return []

            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)

            if not rows:
                log.debug(f"[{state}] offset={offset} — CSV header only, stopping pagination.")
                return []

            # Log column names once (first page only) so NIC col issues are visible
            if offset == 0:
                log.info(f"[{state}] CSV columns: {list(rows[0].keys())}")

            return rows

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as exc:
            wait = RETRY_BASE_SEC * (2 ** (attempt - 1))
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
    df = pd.read_excel(NIC_CODES_FILE, dtype=str)
    df.columns = df.columns.str.strip()

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
    all_sheets = pd.read_excel(DEMAND_FILE, sheet_name=None)

    df      = None
    nic_col = None
    cat_col = None

    for sheet_name, sheet_df in all_sheets.items():
        sheet_df.columns = sheet_df.columns.str.strip()
        _nic = next((c for c in sheet_df.columns if "nic" in c.lower()), None)
        _cat = next((c for c in sheet_df.columns if "cat" in c.lower()), None)
        if _nic and _cat:
            df      = sheet_df
            nic_col = _nic
            cat_col = _cat
            log.info(
                f"Category mapping: using sheet {sheet_name!r}  "
                f"(cols: {nic_col!r}, {cat_col!r})"
            )
            break

    if df is None:
        raise ValueError(
            f"Cannot find NIC or Category columns in any sheet of {DEMAND_FILE}."
        )

    def _norm_nic(x):
        s = str(x).strip()
        if not s or s == "nan":
            return None
        try:
            return str(int(float(s))).zfill(5)
        except (ValueError, OverflowError):
            return None

    df[nic_col] = df[nic_col].apply(_norm_nic)

    mapping = (
        df.dropna(subset=[nic_col, cat_col])
        .groupby(nic_col)[cat_col]
        .agg(lambda x: x.mode().iloc[0])
        .to_dict()
    )

    log.info(
        f"Loaded {len(mapping)} NIC→Category mappings from {DEMAND_FILE}  "
        f"(col: {nic_col!r})"
    )
    return mapping

# ─────────────────────────────────────────────────────────────────────────────
#  NIC CODE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
def _find_nic_column(columns) -> str | None:
    """
    FIX 2: Broadened keyword scan.
    The API returns 'NICCode5Digit' (confirmed in screenshot response body).
    We now match any column whose lowercased name contains both 'nic' and '5',
    or just 'nic' with 'digit', or the legacy patterns — whichever comes first.
    """
    log.info(f"  API columns seen: {list(columns)}")
    cols_lower = {c.lower(): c for c in columns}

    # Priority 1: column containing 'nic' + '5' (e.g. "NICCode5Digit", "nic5digit")
    for orig_lower, orig in cols_lower.items():
        if "nic" in orig_lower and "5" in orig_lower:
            log.info(f"  → NIC column matched (priority 1): {orig!r}")
            return orig

    # Priority 2: column containing 'nic' + 'digit' (e.g. "NICDigit")
    for orig_lower, orig in cols_lower.items():
        if "nic" in orig_lower and "digit" in orig_lower:
            log.info(f"  → NIC column matched (priority 2): {orig!r}")
            return orig

    # Priority 3: legacy keywords
    for kw in ["niccode", "nic_code", "nic"]:
        for orig_lower, orig in cols_lower.items():
            if kw in orig_lower:
                log.info(f"  → NIC column matched (priority 3 / kw={kw!r}): {orig!r}")
                return orig

    log.error(
        f"  Could not find a NIC code column. "
        f"Add a keyword to _find_nic_column() matching one of: {list(columns)}"
    )
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
    Fetches all pages for a state as CSV, filters to matching NIC codes,
    and returns a list of result-row dicts.
    """
    total = fetch_total(state)
    if total == 0:
        log.info(f"[{state}] No records — skipping.")
        return []

    total_pages = (total + BATCH_SIZE - 1) // BATCH_SIZE
    log.info(f"[{state}] {total:,} records across ~{total_pages} page(s)")

    all_rows: list[dict] = []
    nic_col: str | None = None

    for page_num, offset in enumerate(range(0, total, BATCH_SIZE), start=1):
        rows = fetch_page_csv(state, offset)

        if not rows:
            log.info(f"[{state}] Empty page at offset={offset} — pagination complete.")
            break

        # Discover NIC column name once from the first non-empty page
        if nic_col is None:
            nic_col = _find_nic_column(rows[0].keys())
            if nic_col is None:
                log.error(
                    f"[{state}] Cannot find NIC column. "
                    f"Skipping state. Columns: {list(rows[0].keys())}"
                )
                return []

        all_rows.extend(rows)
        log.info(
            f"[{state}] page {page_num}/{total_pages}  "
            f"({len(all_rows):,}/{total:,} fetched)"
        )

    results: list[dict] = []
    for row in all_rows:
        for code in _extract_nic_codes(row.get(nic_col, "")):
            if code not in nic_set:
                continue
            results.append({
                "State":           str(row.get("State",           state)).strip().title(),
                "District":        str(row.get("District",        "")).strip().title(),
                "Pincode":         str(row.get("Pincode",         "")).strip(),
                "Enterprise_Name": str(row.get("EnterpriseName",  "")).strip().title(),
                "NIC_Code":        code,
                "NIC_Description": nic_desc.get(code, ""),
                "Category":        cat_map.get(code, "Uncategorised"),
                "Enterprise_Type": str(row.get("EnterpriseType",  "")).strip().title(),
                "Major_Activity":  str(row.get("MajorActivity",   "")).strip().title(),
            })

    log.info(
        f"[{state}] {len(results):,} matching supplier rows "
        f"(from {len(all_rows):,} total records)"
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
    os.replace(tmp, CHECKPOINT)

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

    nic_set, nic_desc = load_nic_codes()
    cat_map           = load_category_mapping()

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
    print(f"  Resource ID         : {RESOURCE_ID}")
    print(f"  NIC codes loaded    : {len(nic_set)}")
    print(f"  States pending      : {len(pending)}")
    if skipped:
        print(f"  States skipped      : {skipped}  (already completed)")
    print(f"  Batch size          : {BATCH_SIZE} records/page")
    print(f"  Request gap         : {MIN_REQUEST_GAP}s  (~{int(3600/MIN_REQUEST_GAP)}/hr)")
    print(f"  Output              : {OUTPUT_DIR}/suppliers_<State>.csv")
    print(f"{'='*62}\n")

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

        if not args.state:
            _save_checkpoint({"completed": sorted(completed), "failed": failed})

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

    if still_pending or failed:
        sys.exit(2)
    else:
        if not args.state and not args.dry_run and os.path.exists(CHECKPOINT):
            os.remove(CHECKPOINT)
            log.info("Checkpoint cleared — clean full run complete.")
        sys.exit(0)


if __name__ == "__main__":
    main()
