"""
MSME Supplier Fetcher  —  Production v6
========================================
Fetches MSME registered units from data.gov.in, filters by NIC codes
defined in data/Key_NIC_Codes_List.xlsx, maps categories from
data/Demand_Excel_Filled.xlsx, and writes one
    data/suppliers/suppliers_<State>.xlsx
per state.

Rate limit: data.gov.in allows 1,000 requests/hour (rolling window).
This script enforces a 4.5s minimum gap → ~800 req/hr, giving ~20%
headroom.

Exit codes (read by GitHub Actions):
    0  — all states completed cleanly
    2  — states still pending (deadline/partial); workflow auto-retriggers
    3  — run limit hit (MAX_RUNS_WITHOUT_PROGRESS); retrigger chain stops

CHANGES vs v5  (v6)
---------------------
FIX 14 — Output format changed from CSV to XLSX.

    Problem: CSV files for large states exceed GitHub's 100 MB per-file
    hard limit, making git storage impossible without LFS.

    Solution: Each state is fetched into a temporary local CSV (using the
    same atomic append + checkpoint logic from v5), then converted to xlsx
    once the state is fully complete. The CSV is ephemeral — it only exists
    on the runner's local disk and is deleted after conversion. Only the
    final xlsx is committed to git.

    Trade-off: A state interrupted mid-pagination by the deadline will have
    its partial CSV discarded. Run 2 re-fetches that state from offset 0.
    This is acceptable since the run is quarterly and correctness is
    preserved — git only ever sees complete xlsx files per state.

Earlier fixes (retained from v5):
    FIX 1  — urllib3 Retry excludes 429 from status_forcelist.
    FIX 4  — 429 backoff: 120 + 60*attempt seconds.
    FIX 5  — NIC codes parsed from Activities JSON column.
    FIX 6  — Batch size 1000, gap 4.5s.
    FIX 7  — Per-page checkpointing + incremental CSV writes.
    FIX 8  — Verify CSV exists before trusting saved offset.
    FIX 9  — Python-side deadline + DeadlineReached exception.
    FIX 10 — Duplicate-row guard on CSV resume (atomic write).
    FIX 11 — Validate page size before treating short pages as end-of-data.
    FIX 12 — Retry git push with exponential backoff (workflow).
    FIX 13 — Run-limit safety valve to stop infinite retrigger loops.

Requirements:
    pip install requests pandas openpyxl

Usage:
    python msme_supplier_fetcher.py               # normal run / resume
    python msme_supplier_fetcher.py --reset       # ignore checkpoint
    python msme_supplier_fetcher.py --state DELHI # single state
    python msme_supplier_fetcher.py --dry-run     # fetch+filter, no writes

Environment variables:
    DATA_GOV_API_KEY         — API key (required in CI)
    DATA_GOV_RESOURCE_ID     — override resource ID
    RUN_DEADLINE_SECONDS     — override 5h 45m deadline (default 20700)
    MAX_RUNS_WITHOUT_PROGRESS — override stall limit (default 3)
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

BATCH_SIZE         = 1000
TIMEOUT_PAGE       = 60
MAX_RETRIES        = 4
RETRY_BASE         = 5
SHORT_PAGE_RETRIES = 3
MIN_REQUEST_GAP: float = 4.5
_last_request_at: float = 0.0

ACTIVITIES_COLUMN = "Activities"

# ── Sentinel exceptions ───────────────────────────────────────────────────────
class DeadlineReached(Exception):
    """Script hit its time limit — exit cleanly with code 2."""

class RunLimitReached(Exception):
    """Too many consecutive runs with no progress — exit with code 3."""

# ── Run deadline ──────────────────────────────────────────────────────────────
RUN_DEADLINE_SECONDS: int = int(os.environ.get("RUN_DEADLINE_SECONDS", 20_700))
_run_start: float = time.monotonic()

# ── Stall limit ───────────────────────────────────────────────────────────────
MAX_RUNS_WITHOUT_PROGRESS: int = int(os.environ.get("MAX_RUNS_WITHOUT_PROGRESS", 3))

# ── Output columns ────────────────────────────────────────────────────────────
OUTPUT_COLUMNS = [
    "State", "District", "Pincode", "Enterprise_Name",
    "Registration_Date", "Address", "NIC_Code", "NIC_Description", "Category",
]

# ─────────────────────────────────────────────────────────────────────────────
#  STATES / UTs
# ─────────────────────────────────────────────────────────────────────────────
STATES_AND_UTS: list[str] = [
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
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv",
    })
    retry = Retry(
        total=3, backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
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
def fetch_page_csv(state: str, offset: int) -> tuple[list[dict], bool]:
    """
    Fetch one batch of records as CSV.
    Returns (rows, is_last_page).
    A short page (< BATCH_SIZE) is retried SHORT_PAGE_RETRIES times
    before being accepted as the genuine last page.
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
            resp = SESSION.get(BASE_URL, params=params, timeout=TIMEOUT_PAGE, verify=False)

            if resp.status_code == 429:
                wait = 120 + (60 * attempt)
                log.warning(
                    f"[{state}] offset={offset} → 429. "
                    f"Waiting {wait}s (attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            text = resp.text.strip()

            if not text:
                return [], True

            reader = csv.DictReader(io.StringIO(text))
            rows   = list(reader)

            if not rows:
                return [], True

            if offset == 0:
                log.info(f"[{state}] CSV columns: {list(rows[0].keys())}")

            return rows, False

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

    raise RuntimeError(f"[{state}] offset={offset} — failed after {MAX_RETRIES} attempts.")

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
        raise ValueError(f"No NIC code column in {NIC_CODES_FILE}. Columns: {list(df.columns)}")
    desc_col = next((c for c in df.columns if "desc" in c.lower()), None)
    df[code_col] = df[code_col].str.strip().str.zfill(5)
    nic_set  = set(df[code_col].dropna().tolist())
    nic_desc = dict(zip(df[code_col], df[desc_col].fillna(""))) if desc_col else {}
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
            log.info(f"Category mapping: sheet {sheet_name!r} (cols: {nic_col!r}, {cat_col!r})")
            break
    if df is None:
        raise ValueError(f"No NIC/Category columns in any sheet of {DEMAND_FILE}.")

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
            log.debug(f"JSON parse failed for Activities value, falling back: {exc}")
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
    state: str, raw_rows: list[dict],
    nic_set: set[str], nic_desc: dict[str, str], cat_map: dict[str, str],
) -> list[dict]:
    results: list[dict] = []
    for row in raw_rows:
        for code in _extract_nic_codes(row.get(ACTIVITIES_COLUMN, "")):
            if code not in nic_set:
                continue
            results.append({
                "State":             str(row.get("State",            state)).strip().title(),
                "District":          str(row.get("District",             "")).strip().title(),
                "Pincode":           str(row.get("Pincode",              "")).strip(),
                "Enterprise_Name":   str(row.get("EnterpriseName",       "")).strip().title(),
                "Registration_Date": str(row.get("RegistrationDate",     "")).strip(),
                "Address":           str(row.get("CommunicationAddress", "")).strip().title(),
                "NIC_Code":          code,
                "NIC_Description":   nic_desc.get(code, ""),
                "Category":          cat_map.get(code, "Uncategorised"),
            })
    return results

# ─────────────────────────────────────────────────────────────────────────────
#  ATOMIC CSV APPEND  (ephemeral — local disk only, never committed)
# ─────────────────────────────────────────────────────────────────────────────
def _count_csv_rows(path: str) -> int:
    """Return the number of data rows in a CSV (excludes header). 0 if absent."""
    if not os.path.exists(path):
        return 0
    try:
        with open(path, encoding="utf-8-sig") as f:
            return sum(1 for _ in f) - 1
    except Exception:
        return 0


def _append_to_csv(path: str, records: list[dict]) -> None:
    """
    Atomically append records to the ephemeral state CSV.
    Writes to a staging file, fsyncs, then renames over the real path.
    """
    if not records:
        return

    existing_rows: list[dict] = []
    write_header = True
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                existing_rows = list(reader)
            write_header = False
        except Exception as exc:
            log.warning(f"Could not read existing CSV {path} ({exc}) — will overwrite.")
            existing_rows = []
            write_header = True

    tmp_path = path + ".tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        if existing_rows:
            writer.writerows(existing_rows)
        writer.writerows(records)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, path)

# ─────────────────────────────────────────────────────────────────────────────
#  CSV → XLSX CONVERSION  (FIX 14)
#
#  Called once per state after all pages are fetched. Reads the ephemeral
#  CSV into a DataFrame and writes it as xlsx, then deletes the CSV.
#  Only the xlsx is committed to git.
# ─────────────────────────────────────────────────────────────────────────────
def _convert_csv_to_xlsx(csv_path: str, xlsx_path: str) -> None:
    """Convert the completed ephemeral CSV to xlsx and delete the CSV."""
    log.info(f"Converting {csv_path} → {xlsx_path} ...")
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str)
    # Ensure column order matches OUTPUT_COLUMNS
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[OUTPUT_COLUMNS]
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    size_mb = os.path.getsize(xlsx_path) / 1024 / 1024
    log.info(f"xlsx written: {xlsx_path} ({size_mb:.2f} MB, {len(df):,} rows)")
    os.remove(csv_path)
    log.info(f"Ephemeral CSV deleted: {csv_path}")

# ─────────────────────────────────────────────────────────────────────────────
#  CHECKPOINT HELPERS
#
#  Schema v6 — same as v5, xlsx_path added to in_progress block:
#  {
#    "completed":              ["STATE A", ...],
#    "failed":                 ["STATE C"],
#    "runs_without_progress":  0,
#    "in_progress": {
#      "state":        "UTTAR PRADESH",
#      "next_offset":  47000,
#      "csv_path":     "/tmp/.../suppliers_Uttar_Pradesh.csv",
#      "xlsx_path":    "data/suppliers/suppliers_Uttar_Pradesh.xlsx",
#      "rows_written": 12340
#    }
#  }
# ─────────────────────────────────────────────────────────────────────────────
def _load_checkpoint() -> dict:
    if not os.path.exists(CHECKPOINT):
        return {"completed": [], "failed": [], "in_progress": None, "runs_without_progress": 0}
    try:
        with open(CHECKPOINT, encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("in_progress", None)
        data.setdefault("runs_without_progress", 0)
        ip = data["in_progress"]
        log.info(
            f"Checkpoint: {len(data.get('completed', []))} completed, "
            f"{len(data.get('failed', []))} failed, "
            f"{data['runs_without_progress']} runs without progress"
            + (
                f", resuming {ip['state']} at offset {ip['next_offset']} "
                f"({ip.get('rows_written', 0):,} rows written)"
                if ip else ""
            )
        )
        return data
    except Exception as exc:
        log.warning(f"Could not read checkpoint ({exc}). Starting fresh.")
        return {"completed": [], "failed": [], "in_progress": None, "runs_without_progress": 0}


def _save_checkpoint(cp: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tmp = CHECKPOINT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, CHECKPOINT)

# ─────────────────────────────────────────────────────────────────────────────
#  FILENAME HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _safe_filename(s: str) -> str:
    return (
        s.strip().title()
        .replace(" ", "_").replace("/", "-")
        .replace("\\", "-").replace(":", "")
    )

def _csv_path_for(state: str) -> str:
    """Ephemeral CSV — lives only on the runner's local disk."""
    return os.path.join(OUTPUT_DIR, f"suppliers_{_safe_filename(state)}.csv")

def _xlsx_path_for(state: str) -> str:
    """Final output — committed to git."""
    return os.path.join(OUTPUT_DIR, f"suppliers_{_safe_filename(state)}.xlsx")

# ─────────────────────────────────────────────────────────────────────────────
#  PROCESS ONE STATE
# ─────────────────────────────────────────────────────────────────────────────
def process_state(
    state: str,
    nic_set: set[str], nic_desc: dict[str, str], cat_map: dict[str, str],
    cp: dict,
    dry_run: bool = False,
) -> int:
    """
    Fetch all pages for a state.
    - Appends matching rows to an ephemeral CSV after every page.
    - Saves checkpoint after every page.
    - On completion converts the CSV to xlsx and deletes the CSV.
    - Raises DeadlineReached when the run time limit is hit (partial CSV discarded).
    Returns the number of matching rows written this session.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path  = _csv_path_for(state)
    xlsx_path = _xlsx_path_for(state)

    # ── Determine resume offset ───────────────────────────────────────────
    ip = cp.get("in_progress") or {}
    if ip.get("state") == state and ip.get("next_offset") is not None:
        saved_offset       = ip["next_offset"]
        saved_rows_written = ip.get("rows_written", 0)
        csv_row_count      = _count_csv_rows(csv_path)

        if csv_row_count == 0:
            log.warning(
                f"[{state}] Checkpoint claims offset={saved_offset} but CSV "
                f"is empty/absent — restarting from 0."
            )
            start_offset = 0
            rows_written = 0
        elif csv_row_count != saved_rows_written:
            log.warning(
                f"[{state}] CSV has {csv_row_count:,} rows but checkpoint "
                f"says {saved_rows_written:,} — restarting from 0 to be safe."
            )
            if os.path.exists(csv_path):
                os.remove(csv_path)
            start_offset = 0
            rows_written = 0
        else:
            start_offset = saved_offset
            rows_written = saved_rows_written
            log.info(
                f"[{state}] Resuming from offset={start_offset} "
                f"({rows_written:,} rows confirmed in CSV)"
            )
    else:
        start_offset = 0
        rows_written = 0
        # Clean up any stale files from a previous attempt
        for stale in [csv_path, csv_path + ".tmp"]:
            if os.path.exists(stale):
                os.remove(stale)
                log.info(f"[{state}] Removed stale file: {stale}")

    if not dry_run:
        cp["in_progress"] = {
            "state": state, "next_offset": start_offset,
            "csv_path": csv_path, "xlsx_path": xlsx_path,
            "rows_written": rows_written,
        }
        _save_checkpoint(cp)

    log.info(f"[{state}] Starting pagination from offset={start_offset}.")

    total_raw         = 0
    page_num          = 0
    first_page        = True
    short_page_streak = 0

    for offset in range(start_offset, 10**9, BATCH_SIZE):
        page_num += 1
        rows, _ = fetch_page_csv(state, offset)

        # ── End-of-data detection ─────────────────────────────────────────
        if len(rows) == 0:
            log.info(
                f"[{state}] Empty page at offset={offset} — "
                f"pagination complete ({total_raw:,} raw records fetched)."
            )
            break

        if len(rows) < BATCH_SIZE:
            short_page_streak += 1
            if short_page_streak <= SHORT_PAGE_RETRIES:
                log.warning(
                    f"[{state}] Short page ({len(rows)} rows) at offset={offset} "
                    f"— may be truncated, retrying ({short_page_streak}/{SHORT_PAGE_RETRIES})."
                )
                time.sleep(RETRY_BASE * short_page_streak)
                page_num -= 1
                continue
            else:
                log.info(
                    f"[{state}] Short page confirmed as last page after "
                    f"{SHORT_PAGE_RETRIES} retries."
                )
        else:
            short_page_streak = 0

        # Validate Activities column (once, on first page)
        if first_page:
            first_page = False
            if ACTIVITIES_COLUMN not in rows[0]:
                log.error(
                    f"[{state}] '{ACTIVITIES_COLUMN}' column not found. "
                    f"Columns: {list(rows[0].keys())}. Skipping state."
                )
                cp["in_progress"] = None
                if not dry_run:
                    _save_checkpoint(cp)
                return rows_written

        total_raw    += len(rows)
        page_records  = _filter_rows(state, rows, nic_set, nic_desc, cat_map)

        # Atomic CSV append (ephemeral)
        if not dry_run and page_records:
            _append_to_csv(csv_path, page_records)

        rows_written += len(page_records)
        next_offset   = offset + BATCH_SIZE

        # Checkpoint saved AFTER the CSV rename
        if not dry_run:
            cp["in_progress"] = {
                "state": state, "next_offset": next_offset,
                "csv_path": csv_path, "xlsx_path": xlsx_path,
                "rows_written": rows_written,
            }
            _save_checkpoint(cp)

        log.info(
            f"[{state}] page {page_num} (offset={offset}) — "
            f"{len(page_records)} matches this page, "
            f"{rows_written:,} total written, "
            f"{total_raw:,} raw records fetched"
        )

        # Deadline check — after checkpoint so progress is always saved
        elapsed = time.monotonic() - _run_start
        if elapsed >= RUN_DEADLINE_SECONDS:
            log.info(
                f"[{state}] Deadline reached after {elapsed/3600:.2f}h — "
                f"stopping at offset={next_offset}. "
                f"Partial CSV will be discarded; state will re-fetch next run."
            )
            # Clean up partial CSV — don't leave half-built files around
            for stale in [csv_path, csv_path + ".tmp"]:
                if os.path.exists(stale):
                    os.remove(stale)
            raise DeadlineReached()

        # Accept short page as last page after retries
        if len(rows) < BATCH_SIZE:
            break

    # ── State complete — convert CSV → xlsx ───────────────────────────────
    if not dry_run:
        if os.path.exists(csv_path):
            _convert_csv_to_xlsx(csv_path, xlsx_path)
        else:
            log.info(f"[{state}] No matching rows — no xlsx produced.")

    log.info(
        f"[{state}] Complete — {rows_written:,} matching rows "
        f"(from {total_raw:,} total records)"
    )
    return rows_written

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="MSME Supplier Fetcher v6")
    parser.add_argument("--reset",   action="store_true", help="Ignore checkpoint and restart")
    parser.add_argument("--state",   type=str, default=None, help="Run a single state")
    parser.add_argument("--dry-run", action="store_true", help="Fetch+filter, no file writes")
    args = parser.parse_args()

    if not API_KEY:
        log.error("DATA_GOV_API_KEY is not set. Aborting.")
        sys.exit(1)

    nic_set, nic_desc = load_nic_codes()
    cat_map           = load_category_mapping()

    if args.state:
        target = args.state.strip().upper()
        if target not in STATES_AND_UTS:
            log.error(f"Unknown state: {target!r}.")
            sys.exit(1)
        cp      = {"completed": [], "failed": [], "in_progress": None, "runs_without_progress": 0}
        pending = [target]
    else:
        if args.reset:
            cp = {"completed": [], "failed": [], "in_progress": None, "runs_without_progress": 0}
            log.info("Reset flag set — ignoring any existing checkpoint.")
        else:
            cp = _load_checkpoint()

        runs_without_progress = cp.get("runs_without_progress", 0)
        if runs_without_progress >= MAX_RUNS_WITHOUT_PROGRESS:
            log.error(
                f"No new states completed in the last {runs_without_progress} runs. "
                f"Stopping retrigger chain to avoid infinite loop. "
                f"Investigate failed states, then use --reset or delete the checkpoint to resume."
            )
            raise RunLimitReached()

        completed = set(cp.get("completed", []))
        ip_state  = (cp.get("in_progress") or {}).get("state")
        remaining = [s for s in STATES_AND_UTS if s not in completed]
        if ip_state and ip_state in remaining:
            pending = [ip_state] + [s for s in remaining if s != ip_state]
        else:
            pending = remaining

    skipped = len(STATES_AND_UTS) - len(pending) if not args.state else 0

    print(f"\n{'═'*64}")
    print(f"  MSME Supplier Fetcher  v6  {'[DRY RUN]' if args.dry_run else ''}")
    print(f"  Resource ID           : {RESOURCE_ID}")
    print(f"  NIC codes loaded      : {len(nic_set)}")
    print(f"  States pending        : {len(pending)}  (skipped: {skipped})")
    print(f"  Batch size            : {BATCH_SIZE} records/page")
    print(f"  Request gap           : {MIN_REQUEST_GAP}s → ~{int(3600/MIN_REQUEST_GAP)} req/hr (limit 1,000)")
    print(f"  Deadline              : {RUN_DEADLINE_SECONDS/3600:.2f}h")
    print(f"  Stall limit           : {MAX_RUNS_WITHOUT_PROGRESS} runs without progress")
    print(f"  Output folder         : {OUTPUT_DIR}/suppliers_<State>.xlsx")
    print(f"{'═'*64}\n")

    total_suppliers    = 0
    completed_this_run = 0

    for i, state in enumerate(pending, 1):
        log.info(f"[{i:02d}/{len(pending)}] ── {state.title()} ──")
        try:
            rows_written = process_state(
                state, nic_set, nic_desc, cat_map, cp, dry_run=args.dry_run
            )
            total_suppliers    += rows_written
            completed_this_run += 1

            completed_list = cp.get("completed", [])
            if state not in completed_list:
                completed_list.append(state)
            cp["completed"]             = sorted(completed_list)
            cp["failed"]                = [s for s in cp.get("failed", []) if s != state]
            cp["in_progress"]           = None
            cp["runs_without_progress"] = 0

            if not args.state and not args.dry_run:
                _save_checkpoint(cp)

        except DeadlineReached:
            log.info("Deadline caught in main loop — stopping immediately.")
            if completed_this_run == 0 and not args.state and not args.dry_run:
                cp["runs_without_progress"] = cp.get("runs_without_progress", 0) + 1
                _save_checkpoint(cp)
            break

        except Exception as exc:
            log.error(f"[{state}] FAILED: {exc}")
            failed = cp.get("failed", [])
            if state not in failed:
                failed.append(state)
            cp["failed"] = failed
            if not args.state and not args.dry_run:
                _save_checkpoint(cp)

    # Increment stall counter if nothing completed (non-deadline exit)
    if completed_this_run == 0 and not args.state and not args.dry_run:
        if not any(isinstance(sys.exc_info()[1], DeadlineReached) for _ in [None]):
            cp["runs_without_progress"] = cp.get("runs_without_progress", 0) + 1
            _save_checkpoint(cp)

    completed_set = set(cp.get("completed", []))
    still_pending = [s for s in STATES_AND_UTS if s not in completed_set]
    failed        = cp.get("failed", [])

    print(f"\n{'═'*64}")
    print(f"  Total supplier rows saved : {total_suppliers:,}")
    print(f"  States completed this run : {completed_this_run}")
    print(f"  Output folder             : {OUTPUT_DIR}/")
    if failed:
        print(f"\n  States FAILED (will retry on next run):")
        for s in failed:
            print(f"    ✗  {s.title()}")
    if still_pending:
        print(f"\n  States still pending:")
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
            log.info("Checkpoint cleared — full run complete.")
        sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except RunLimitReached:
        sys.exit(3)


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
