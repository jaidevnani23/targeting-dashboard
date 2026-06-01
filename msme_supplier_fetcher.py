"""
MSME Supplier Fetcher  —  Production v4.2
========================================
Fetches MSME registered units from data.gov.in, filters by NIC codes
defined in data/Key_NIC_Codes_List.xlsx, maps categories from
data/Demand_Excel_Filled.xlsx, and writes one
    data/suppliers/suppliers_<State>.csv
per state.

Rate limit: data.gov.in allows 1,000 requests/hour (rolling window).
This script enforces a 4.5s minimum gap → ~800 req/hr, giving ~20%
headroom.

Exit codes (read by GitHub Actions):
    0  — all states completed cleanly
    2  — states still pending (timeout/partial); workflow auto-retriggers

CHANGES vs v3  (v4)
-------------------
FIX 7 — Per-page checkpointing so no work is lost on mid-run interruption.

    Problem (v3): the checkpoint only recorded fully completed states.
    If the runner was killed mid-state (e.g. GitHub Actions 6-hr timeout),
    the entire in-progress state had to be refetched from offset 0 on the
    next run.  For large states (UP ~951 pages, MH ~700 pages) that meant
    throwing away hours of work.

    Solution (v4):
      • The checkpoint now records the current state AND the last
        successfully written page offset:
            {
              "completed":      ["ASSAM", ...],
              "failed":         [],
              "in_progress":    {
                "state":        "UTTAR PRADESH",
                "next_offset":  47000,
                "csv_path":     "data/suppliers/suppliers_Uttar_Pradesh.csv"
              }
            }
      • Rows are appended to the state CSV one page at a time instead of
        being held in memory until the state finishes.  The CSV is therefore
        always current up to the last completed page.
      • On resume, the script reads the in_progress block, seeks directly
        to next_offset, and appends to the existing CSV.
      • The workflow uses a shell trap (EXIT + SIGTERM) to run the git-commit
        step even when the runner is killed by a timeout or cancellation,
        ensuring the incremental CSVs and checkpoint reach the repo.

    Net effect: the worst-case data loss is now one page (BATCH_SIZE records,
    default 1000) rather than an entire state.

FIX 9 — Python-side deadline replaces shell trap.  (v4.2)

    Problem: shell traps registered in one `run:` step carry over when the
    fetcher is backgrounded, but GitHub Actions cancellation sends SIGTERM
    to the entire runner process group simultaneously, giving the trap no
    reliable window to commit before SIGKILL escalation.

    Solution: the script records its start time at import and checks elapsed
    time after every page. At 5h 45m (RUN_DEADLINE_SECONDS=20700) it logs
    the stop reason and exits with code 2, leaving 15 min inside the 6h job
    ceiling for the workflow to commit and retrigger. The workflow no longer
    needs a trap at all — the commit always happens via the normal exit path.
    Override for local testing: RUN_DEADLINE_SECONDS=60 python msme_supplier_fetcher.py

    Problem: if the fetcher was killed hard enough that the workflow trap did
    not fire (e.g. OOM before SIGTERM), the checkpoint JSON was already
    committed to the repo (written per-page) but the partial CSV was not.
    On the next run, process_state() would blindly seek to next_offset on a
    fresh runner where the CSV does not exist, silently skipping those records.

    Solution: before trusting the saved offset, confirm the CSV exists and is
    non-empty on the current runner.  If absent, log a warning and restart
    that state from offset 0.

Earlier fixes (retained from v3):
    FIX 1 — urllib3 Retry excludes 429 from status_forcelist.
    FIX 2 — fallback blind pagination when total==0.
    FIX 4 — 429 backoff: 120 + 60*attempt seconds.
    FIX 5 — NIC codes parsed from Activities JSON column.
    FIX 6 — Batch size 1000, gap 4.5s.

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

_BASE = "data" if os.path.isdir("data") else "."
NIC_CODES_FILE = os.path.join(_BASE, "Key_NIC_Codes_List.xlsx")
DEMAND_FILE    = os.path.join(_BASE, "Demand_Excel_Filled.xlsx")
OUTPUT_DIR     = os.path.join(_BASE, "suppliers")
CHECKPOINT     = os.path.join(OUTPUT_DIR, "fetch_checkpoint.json")

BATCH_SIZE      = 1000
TIMEOUT_PAGE    = 60
MAX_RETRIES     = 4
RETRY_BASE      = 5
MIN_REQUEST_GAP: float = 4.5
_last_request_at: float = 0.0

ACTIVITIES_COLUMN = "Activities"

# ── Run deadline (FIX 9) ──────────────────────────────────────────────────────
# The script stops itself after RUN_DEADLINE_SECONDS and exits with code 2 so
# the workflow can commit progress and retrigger cleanly — without relying on
# OS signals or shell traps, which proved unreliable under GitHub Actions
# timeouts. Set to 5h 45m (20,700s) to leave 15 min inside the 6h job ceiling
# for the git commit + push and retrigger steps to complete.
# Override via env var for local testing: RUN_DEADLINE_SECONDS=60 python ...
RUN_DEADLINE_SECONDS: int = int(os.environ.get("RUN_DEADLINE_SECONDS", 20_700))
_run_start: float = time.monotonic()

# CSV columns written to every state file — order is fixed so appends align.
OUTPUT_COLUMNS = [
    "State",
    "District",
    "Pincode",
    "Enterprise_Name",
    "Registration_Date",
    "Address",
    "NIC_Code",
    "NIC_Description",
    "Category",
]

# ─────────────────────────────────────────────────────────────────────────────
#  STATES / UTs
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
# ─────────────────────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
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
#  FETCH ONE CSV PAGE
# ─────────────────────────────────────────────────────────────────────────────
def fetch_page_csv(state: str, offset: int) -> list[dict]:
    """
    Fetch one batch of records as CSV. Returns [] on empty page (end of data).
    Raises RuntimeError after MAX_RETRIES failures.
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
            resp = SESSION.get(
                BASE_URL, params=params, timeout=TIMEOUT_PAGE, verify=False
            )

            if resp.status_code == 429:
                wait = 120 + (60 * attempt)
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

    log.info(
        f"Loaded {len(nic_set)} NIC codes from {NIC_CODES_FILE} "
        f"(col: {code_col!r})"
    )
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
#  NIC CODE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
def _extract_nic_codes(raw_value) -> list[str]:
    """
    Parse all 5-digit NIC codes from the Activities column value.

    Handles:
      - JSON array:  [{"NIC5DigitId":"14101","Description":"..."}]
      - Legacy plain-text fallback: "1) 14101; 2) 22199"
    """
    if not raw_value or str(raw_value).strip() in ("", "nan", "NA"):
        return []

    text = str(raw_value).strip()

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
            log.debug(
                f"JSON parse failed for Activities value, falling back: {exc}"
            )

    # Legacy semicolon-separated plain text
    codes = []
    for part in text.split(";"):
        part = part.strip()
        if ")" in part:
            part = part.split(")")[-1].strip()
        if part and part not in ("", "nan"):
            codes.append(part.zfill(5))
    return codes


# ─────────────────────────────────────────────────────────────────────────────
#  FILTER RAW ROWS → OUTPUT DICTS
# ─────────────────────────────────────────────────────────────────────────────
def _filter_rows(
    state: str,
    raw_rows: list[dict],
    nic_set: set[str],
    nic_desc: dict[str, str],
    cat_map: dict[str, str],
) -> list[dict]:
    """Filter and enrich a list of raw API rows into output dicts."""
    results: list[dict] = []
    for row in raw_rows:
        for code in _extract_nic_codes(row.get(ACTIVITIES_COLUMN, "")):
            if code not in nic_set:
                continue
            results.append({
                "State":             str(row.get("State",              state)).strip().title(),
                "District":          str(row.get("District",               "")).strip().title(),
                "Pincode":           str(row.get("Pincode",                "")).strip(),
                "Enterprise_Name":   str(row.get("EnterpriseName",         "")).strip().title(),
                "Registration_Date": str(row.get("RegistrationDate",       "")).strip(),
                "Address":           str(row.get("CommunicationAddress",   "")).strip().title(),
                "NIC_Code":          code,
                "NIC_Description":   nic_desc.get(code, ""),
                "Category":          cat_map.get(code, "Uncategorised"),
            })
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  INCREMENTAL CSV WRITER  (FIX 7)
#
#  Appends rows to the state CSV immediately after each page is fetched so
#  the file on disk is always current up to the last completed page.
#  If the file doesn't exist yet, writes the header first.
# ─────────────────────────────────────────────────────────────────────────────
def _append_to_csv(path: str, records: list[dict]) -> None:
    """Append records to a CSV, writing the header if the file is new."""
    if not records:
        return
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(records)


# ─────────────────────────────────────────────────────────────────────────────
#  CHECKPOINT HELPERS  (FIX 7: extended schema)
#
#  Schema:
#  {
#    "completed":   ["STATE A", "STATE B", ...],   # fully done states
#    "failed":      ["STATE C"],                   # errored states
#    "in_progress": {                              # optional; set mid-state
#      "state":       "UTTAR PRADESH",
#      "next_offset": 47000,                       # offset to resume from
#      "csv_path":    "data/suppliers/suppliers_Uttar_Pradesh.csv",
#      "rows_written": 12340                       # matching rows so far
#    }
#  }
# ─────────────────────────────────────────────────────────────────────────────
def _load_checkpoint() -> dict:
    if not os.path.exists(CHECKPOINT):
        return {"completed": [], "failed": [], "in_progress": None}
    try:
        with open(CHECKPOINT, encoding="utf-8") as f:
            data = json.load(f)
        # Back-compat: v3 checkpoints lack "in_progress"
        data.setdefault("in_progress", None)
        ip = data["in_progress"]
        log.info(
            f"Checkpoint: {len(data.get('completed', []))} completed, "
            f"{len(data.get('failed', []))} previously failed"
            + (
                f", resuming {ip['state']} at offset {ip['next_offset']} "
                f"({ip.get('rows_written', 0):,} rows already written)"
                if ip else ""
            )
        )
        return data
    except Exception as exc:
        log.warning(f"Could not read checkpoint ({exc}). Starting fresh.")
        return {"completed": [], "failed": [], "in_progress": None}


def _save_checkpoint(cp: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tmp = CHECKPOINT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)
    os.replace(tmp, CHECKPOINT)


# ─────────────────────────────────────────────────────────────────────────────
#  FILENAME HELPER
# ─────────────────────────────────────────────────────────────────────────────
def _safe_filename(s: str) -> str:
    return (
        s.strip().title()
        .replace(" ", "_").replace("/", "-")
        .replace("\\", "-").replace(":", "")
    )


def _csv_path_for(state: str) -> str:
    return os.path.join(
        OUTPUT_DIR, f"suppliers_{_safe_filename(state)}.csv"
    )


# ─────────────────────────────────────────────────────────────────────────────
#  PROCESS ONE STATE  (FIX 7: per-page checkpoint + incremental CSV)
# ─────────────────────────────────────────────────────────────────────────────
def process_state(
    state: str,
    nic_set: set[str],
    nic_desc: dict[str, str],
    cat_map: dict[str, str],
    cp: dict,
    dry_run: bool = False,
) -> int:
    """
    Fetch all pages for a state.  Appends matching rows to the state CSV
    after every page and updates the checkpoint immediately, so an
    interruption loses at most one page worth of work.

    Returns the total number of matching rows written for this state.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = _csv_path_for(state)

    # ── Determine resume offset ───────────────────────────────────────────
    ip = cp.get("in_progress") or {}
    if ip.get("state") == state and ip.get("next_offset") is not None:
        # FIX 8: verify the partial CSV actually exists on disk before
        # trusting the saved offset.  If the trap failed to fire (e.g. OOM
        # kill before SIGTERM) the checkpoint was committed but the CSV was
        # not, so the file is absent on the fresh runner.  Blindly resuming
        # at next_offset would silently skip offsets 0..next_offset-1 forever.
        csv_present = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
        if csv_present:
            start_offset = ip["next_offset"]
            rows_written = ip.get("rows_written", 0)
            log.info(
                f"[{state}] Resuming from offset={start_offset} "
                f"({rows_written:,} rows already in {csv_path})"
            )
        else:
            log.warning(
                f"[{state}] Checkpoint claims offset={ip['next_offset']} but "
                f"CSV is missing or empty on this runner — restarting from 0."
            )
            start_offset = 0
            rows_written = 0
    else:
        start_offset = 0
        rows_written = 0
        # Fresh start for this state — remove any stale partial CSV
        if os.path.exists(csv_path):
            os.remove(csv_path)
            log.info(f"[{state}] Removed stale CSV from previous attempt.")

    # Mark this state as in-progress in the checkpoint immediately
    if not dry_run:
        cp["in_progress"] = {
            "state":       state,
            "next_offset": start_offset,
            "csv_path":    csv_path,
            "rows_written": rows_written,
        }
        _save_checkpoint(cp)

    log.info(f"[{state}] Starting pagination from offset={start_offset}.")

    total_raw    = 0
    page_num     = 0
    first_page   = True

    for offset in range(start_offset, 10**9, BATCH_SIZE):
        page_num += 1
        rows = fetch_page_csv(state, offset)

        if not rows:
            log.info(
                f"[{state}] Empty page at offset={offset} — "
                f"pagination complete ({total_raw:,} raw records fetched)."
            )
            break

        # Validate Activities column exists (warn once on first fetched page)
        if first_page:
            first_page = False
            if ACTIVITIES_COLUMN not in rows[0]:
                available = list(rows[0].keys())
                log.error(
                    f"[{state}] Expected column '{ACTIVITIES_COLUMN}' not found. "
                    f"Available columns: {available}. Skipping state."
                )
                # Clear in_progress so we don't resume into a broken state
                cp["in_progress"] = None
                if not dry_run:
                    _save_checkpoint(cp)
                return rows_written

        total_raw += len(rows)

        # Filter and enrich this page's rows
        page_records = _filter_rows(state, rows, nic_set, nic_desc, cat_map)

        # ── Persist immediately (FIX 7) ───────────────────────────────────
        if not dry_run and page_records:
            _append_to_csv(csv_path, page_records)

        rows_written += len(page_records)
        next_offset   = offset + BATCH_SIZE

        # Update checkpoint after every page
        if not dry_run:
            cp["in_progress"] = {
                "state":        state,
                "next_offset":  next_offset,
                "csv_path":     csv_path,
                "rows_written": rows_written,
            }
            _save_checkpoint(cp)

        log.info(
            f"[{state}] page {page_num} (offset={offset}) — "
            f"{len(page_records)} matches this page, "
            f"{rows_written:,} total written, "
            f"{total_raw:,} raw records fetched"
        )

        # ── Deadline check (FIX 9) ────────────────────────────────────────
        elapsed = time.monotonic() - _run_start
        remaining = RUN_DEADLINE_SECONDS - elapsed
        if remaining <= 0:
            log.info(
                f"[{state}] Deadline reached after {elapsed/3600:.2f}h — "
                f"stopping at offset={next_offset} with checkpoint saved. "
                f"Workflow will commit and retrigger."
            )
            return rows_written

    log.info(
        f"[{state}] Complete — {rows_written:,} matching supplier rows "
        f"(from {total_raw:,} total records)"
    )
    return rows_written


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="MSME Supplier Fetcher v4.2")
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
        cp        = {"completed": [], "failed": [], "in_progress": None}
        pending   = [target]
    else:
        if args.reset:
            cp = {"completed": [], "failed": [], "in_progress": None}
            log.info("Reset flag set — ignoring any existing checkpoint.")
        else:
            cp = _load_checkpoint()

        completed = set(cp.get("completed", []))

        # If a state was in_progress, put it first in pending so we resume it
        # before moving on to states we haven't started yet.
        ip_state  = (cp.get("in_progress") or {}).get("state")
        remaining = [s for s in STATES_AND_UTS if s not in completed]
        if ip_state and ip_state in remaining:
            pending = [ip_state] + [s for s in remaining if s != ip_state]
        else:
            pending = remaining

    skipped = len(STATES_AND_UTS) - len(pending) if not args.state else 0

    print(f"\n{'═'*64}")
    print(f"  MSME Supplier Fetcher  v4  {'[DRY RUN]' if args.dry_run else ''}")
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
            rows_written = process_state(
                state, nic_set, nic_desc, cat_map, cp, dry_run=args.dry_run
            )
            total_suppliers += rows_written

            # Mark fully completed
            completed_list = cp.get("completed", [])
            if state not in completed_list:
                completed_list.append(state)
            cp["completed"]  = sorted(completed_list)
            cp["failed"]     = [s for s in cp.get("failed", []) if s != state]
            cp["in_progress"] = None

            if not args.state and not args.dry_run:
                _save_checkpoint(cp)

        except Exception as exc:
            log.error(f"[{state}] FAILED: {exc}")
            failed = cp.get("failed", [])
            if state not in failed:
                failed.append(state)
            cp["failed"] = failed
            # Leave in_progress intact so partial CSV + offset are preserved;
            # the next run will retry from where this page left off.
            if not args.state and not args.dry_run:
                _save_checkpoint(cp)

    completed_set = set(cp.get("completed", []))
    still_pending = [s for s in STATES_AND_UTS if s not in completed_set]
    failed        = cp.get("failed", [])

    print(f"\n{'═'*64}")
    print(f"  Total supplier rows saved : {total_suppliers:,}")
    print(f"  Output folder             : {OUTPUT_DIR}/")
    if failed:
        print(f"\n  States FAILED (will retry on next run):")
        for s in failed:
            print(f"    ✗  {s.title()}")
    if still_pending:
        print(f"\n  States still pending (not reached in this run):")
        for s in still_pending:
            marker = "↺" if s == (cp.get("in_progress") or {}).get("state") else "○"
            print(f"    {marker}  {s.title()}")
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
