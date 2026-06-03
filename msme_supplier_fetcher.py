"""
MSME Supplier Fetcher  —  Production v8
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

CHANGES vs v7  (v8)
---------------------
FIX 16 — Partial state data saved on deadline instead of discarded.

    Problem: When the deadline hit mid-state, the partial CSV was deleted
    and the state restarted from offset 0 next run. For large states like
    Maharashtra (~7,000,000 records, ~8.75 hours to fetch), this meant
    perpetually re-fetching the same data across every run with zero
    progress.

    Solution:
    (a) On deadline, convert the partial CSV to xlsx and commit it
        immediately (same as a completed state), then raise DeadlineReached.
    (b) On resume, detect an existing partial xlsx for the in-progress
        state and read its row count so pagination continues from the
        correct offset rather than restarting from 0.
    (c) _convert_csv_to_xlsx() now merges new rows into an existing xlsx
        if one is present (run 2 appends to run 1's partial xlsx, etc.).
    (d) The runs_without_progress stall counter is reset to 0 when a
        partial xlsx was saved on deadline, so the retrigger chain does
        not stop prematurely for large states that span multiple runs.

FIX 17 — Multi-sheet xlsx support for states exceeding Excel's row limit.

    Problem: Excel has a hard limit of 1,048,576 rows per sheet. A state
    with millions of filtered rows would silently truncate on write.

    Solution: _write_xlsx_multisheet() splits rows across Data_1, Data_2,
    ... sheets as needed. Each sheet gets the full header row. The merge
    logic in _convert_csv_to_xlsx() reads all sheets when loading an
    existing xlsx so no previously saved rows are lost.

Earlier fixes (retained from v7):
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
    FIX 14 — Output format changed from CSV to XLSX.
    FIX 15 — Per-state git commit after each state xlsx is produced.
             git config must be set before this script runs (see workflow).
    FIX 16 — Partial state data saved on deadline instead of discarded.
             Stall counter resets when partial xlsx saved (not just on
             full state completion).
    FIX 17 — Multi-sheet xlsx support for states exceeding Excel row limit.

Requirements:
    pip install requests pandas openpyxl

Usage:
    python msme_supplier_fetcher.py               # normal run / resume
    python msme_supplier_fetcher.py --reset       # ignore checkpoint
    python msme_supplier_fetcher.py --state DELHI # single state
    python msme_supplier_fetcher.py --dry-run     # fetch+filter, no writes

Environment variables:
    DATA_GOV_API_KEY          — API key (required in CI)
    DATA_GOV_RESOURCE_ID      — override resource ID
    RUN_DEADLINE_SECONDS      — override 5h 45m deadline (default 20700)
    MAX_RUNS_WITHOUT_PROGRESS — override stall limit (default 10)
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import subprocess
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

# Excel hard limit per sheet (leave 1 row for header)
EXCEL_MAX_ROWS = 1_048_575

# ── Sentinel exceptions ───────────────────────────────────────────────────────
class DeadlineReached(Exception):
    """Script hit its time limit — exit cleanly with code 2."""

class RunLimitReached(Exception):
    """Too many consecutive runs with no progress — exit with code 3."""

# ── Run deadline ──────────────────────────────────────────────────────────────
RUN_DEADLINE_SECONDS: int = int(os.environ.get("RUN_DEADLINE_SECONDS", 20_700))
_run_start: float = time.monotonic()

# ── Stall limit ───────────────────────────────────────────────────────────────
MAX_RUNS_WITHOUT_PROGRESS: int = int(os.environ.get("MAX_RUNS_WITHOUT_PROGRESS", 10))

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
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                existing_rows = list(reader)
        except Exception as exc:
            log.warning(f"Could not read existing CSV {path} ({exc}) — will overwrite.")
            existing_rows = []

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
#  MULTI-SHEET XLSX WRITER  (FIX 17)
# ─────────────────────────────────────────────────────────────────────────────
def _write_xlsx_multisheet(df: pd.DataFrame, xlsx_path: str) -> None:
    """Write df to xlsx, splitting into multiple sheets if rows exceed Excel limit."""
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        if len(df) <= EXCEL_MAX_ROWS:
            df.to_excel(writer, index=False, sheet_name="Data_1")
        else:
            num_sheets = (len(df) + EXCEL_MAX_ROWS - 1) // EXCEL_MAX_ROWS
            log.info(
                f"Row count {len(df):,} exceeds Excel limit — "
                f"splitting across {num_sheets} sheets."
            )
            for i in range(num_sheets):
                chunk      = df.iloc[i * EXCEL_MAX_ROWS : (i + 1) * EXCEL_MAX_ROWS]
                sheet_name = f"Data_{i + 1}"
                chunk.to_excel(writer, index=False, sheet_name=sheet_name)
                log.info(f"  Sheet {sheet_name}: {len(chunk):,} rows")

    size_mb = os.path.getsize(xlsx_path) / 1024 / 1024
    log.info(f"xlsx written: {xlsx_path} ({size_mb:.2f} MB, {len(df):,} rows total)")


def _read_xlsx_all_sheets(xlsx_path: str) -> pd.DataFrame:
    """Read all sheets from an xlsx and return a single concatenated DataFrame."""
    all_sheets = pd.read_excel(xlsx_path, sheet_name=None, dtype=str, engine="openpyxl")
    frames = []
    for _, df in all_sheets.items():
        for col in OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        frames.append(df[OUTPUT_COLUMNS])
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return pd.concat(frames, ignore_index=True)

# ─────────────────────────────────────────────────────────────────────────────
#  CSV → XLSX CONVERSION  (FIX 16 + FIX 17)
# ─────────────────────────────────────────────────────────────────────────────
def _convert_csv_to_xlsx(csv_path: str, xlsx_path: str) -> None:
    """
    Convert the completed/partial ephemeral CSV to xlsx.
    Merges with an existing xlsx if one is present (partial run continuation).
    Splits across multiple sheets if total rows exceed Excel's limit.
    Deletes the CSV on success.
    """
    log.info(f"Converting {csv_path} → {xlsx_path} ...")
    df_new = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str)
    for col in OUTPUT_COLUMNS:
        if col not in df_new.columns:
            df_new[col] = ""
    df_new = df_new[OUTPUT_COLUMNS]

    if os.path.exists(xlsx_path):
        log.info(
            f"Existing xlsx found — merging {len(df_new):,} new rows "
            f"into existing data."
        )
        try:
            df_existing = _read_xlsx_all_sheets(xlsx_path)
            df = pd.concat([df_existing, df_new], ignore_index=True)
            log.info(
                f"Merged: {len(df_existing):,} existing + "
                f"{len(df_new):,} new = {len(df):,} total rows."
            )
        except Exception as exc:
            log.warning(
                f"Could not read existing xlsx ({exc}) — "
                f"writing new rows only (existing data may be lost)."
            )
            df = df_new
    else:
        df = df_new

    _write_xlsx_multisheet(df, xlsx_path)
    os.remove(csv_path)
    log.info(f"Ephemeral CSV deleted: {csv_path}")

# ─────────────────────────────────────────────────────────────────────────────
#  PER-STATE GIT COMMIT  (FIX 15)
# ─────────────────────────────────────────────────────────────────────────────
def _git_commit_xlsx(xlsx_path: str, state: str) -> None:
    """Commit and push a single completed/partial state xlsx file."""
    try:
        subprocess.run(["git", "add", xlsx_path], check=True)

        diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if diff.returncode == 0:
            log.info(f"[{state}] xlsx already committed — nothing to push.")
            return

        subprocess.run(
            ["git", "commit", "-m", f"chore(data): suppliers {state.title()} [auto]"],
            check=True,
        )
        log.info(f"[{state}] xlsx committed locally.")

    except subprocess.CalledProcessError as e:
        log.error(
            f"[{state}] git add/commit failed: {e} — "
            f"xlsx will be picked up by final commit step."
        )
        return

    for attempt in range(1, 4):
        try:
            subprocess.run(["git", "checkout", "--", "."], check=False)
            subprocess.run(["git", "clean", "-fd"], check=False)
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], check=True)
            subprocess.run(["git", "push"], check=True)
            log.info(f"[{state}] xlsx pushed successfully (attempt {attempt}).")
            return
        except subprocess.CalledProcessError as e:
            wait = attempt * attempt * 10
            log.warning(
                f"[{state}] Push failed (attempt {attempt}/3): {e} — "
                f"retrying in {wait}s."
            )
            time.sleep(wait)

    log.error(
        f"[{state}] Failed to push xlsx after 3 attempts. "
        f"xlsx remains on disk and will be picked up by the workflow's final commit step."
    )

# ─────────────────────────────────────────────────────────────────────────────
#  CHECKPOINT HELPERS
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
    return os.path.join(OUTPUT_DIR, f"suppliers_{_safe_filename(state)}.csv")

def _xlsx_path_for(state: str) -> str:
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
    - On completion OR deadline: converts the CSV to xlsx (merging with any
      existing partial xlsx), commits and pushes immediately.
    - On resume: detects existing partial xlsx and counts its rows so
      pagination resumes correctly without re-fetching.

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
            # CSV is gone — check if a partial xlsx was saved from a prior
            # deadline hit (FIX 16: resume from partial xlsx)
            if os.path.exists(xlsx_path):
                try:
                    df_existing    = _read_xlsx_all_sheets(xlsx_path)
                    xlsx_row_count = len(df_existing)
                    log.info(
                        f"[{state}] No CSV but partial xlsx exists with "
                        f"{xlsx_row_count:,} rows — resuming from "
                        f"offset={saved_offset}."
                    )
                    start_offset = saved_offset
                    rows_written = xlsx_row_count
                except Exception as exc:
                    log.warning(
                        f"[{state}] Could not read existing xlsx ({exc}) — "
                        f"restarting from 0."
                    )
                    start_offset = 0
                    rows_written = 0
            else:
                log.warning(
                    f"[{state}] Checkpoint claims offset={saved_offset} but "
                    f"CSV is empty/absent and no xlsx found — restarting from 0."
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

        if not dry_run and page_records:
            _append_to_csv(csv_path, page_records)

        rows_written += len(page_records)
        next_offset   = offset + BATCH_SIZE

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

        # ── Deadline check ────────────────────────────────────────────────
        elapsed = time.monotonic() - _run_start
        if elapsed >= RUN_DEADLINE_SECONDS:
            log.info(
                f"[{state}] Deadline reached after {elapsed/3600:.2f}h — "
                f"stopping at offset={next_offset}. "
                f"Saving partial xlsx and committing before exit (FIX 16)."
            )
            if not dry_run and os.path.exists(csv_path):
                _convert_csv_to_xlsx(csv_path, xlsx_path)
                _git_commit_xlsx(xlsx_path, state)
            raise DeadlineReached()

        if len(rows) < BATCH_SIZE:
            break

    # ── State complete — convert CSV → xlsx → commit ──────────────────────
    if not dry_run:
        if os.path.exists(csv_path):
            _convert_csv_to_xlsx(csv_path, xlsx_path)
            _git_commit_xlsx(xlsx_path, state)
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
    parser = argparse.ArgumentParser(description="MSME Supplier Fetcher v8")
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
    print(f"  MSME Supplier Fetcher  v8  {'[DRY RUN]' if args.dry_run else ''}")
    print(f"  Resource ID           : {RESOURCE_ID}")
    print(f"  NIC codes loaded      : {len(nic_set)}")
    print(f"  States pending        : {len(pending)}  (skipped: {skipped})")
    print(f"  Batch size            : {BATCH_SIZE} records/page")
    print(f"  Request gap           : {MIN_REQUEST_GAP}s → ~{int(3600/MIN_REQUEST_GAP)} req/hr (limit 1,000)")
    print(f"  Deadline              : {RUN_DEADLINE_SECONDS/3600:.2f}h")
    print(f"  Stall limit           : {MAX_RUNS_WITHOUT_PROGRESS} runs without progress")
    print(f"  Excel row limit       : {EXCEL_MAX_ROWS:,} rows/sheet (splits to multiple sheets if exceeded)")
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
            cp["runs_without_progress"] = 0   # full completion resets stall counter

            if not args.state and not args.dry_run:
                _save_checkpoint(cp)

        except DeadlineReached:
            log.info("Deadline caught in main loop — stopping immediately.")
            if not args.state and not args.dry_run:
                # FIX 16: reset stall counter if partial xlsx was saved,
                # only increment if truly nothing was saved this run
                partial_saved = os.path.exists(_xlsx_path_for(state))
                if completed_this_run > 0 or partial_saved:
                    cp["runs_without_progress"] = 0
                    log.info(
                        f"Progress made this run "
                        f"({'partial xlsx saved' if partial_saved else 'state(s) completed'}) "
                        f"— stall counter reset to 0."
                    )
                else:
                    cp["runs_without_progress"] = cp.get("runs_without_progress", 0) + 1
                    log.warning(
                        f"No progress this run — stall counter now "
                        f"{cp['runs_without_progress']}/{MAX_RUNS_WITHOUT_PROGRESS}."
                    )
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

    # ── Stall counter update for non-deadline exits ───────────────────────
    # Only reached if the loop finished without a DeadlineReached exception.
    # If nothing completed and no partial xlsx exists for any pending state,
    # increment the stall counter (FIX 16).
    if completed_this_run == 0 and not args.state and not args.dry_run:
        completed_set = set(cp.get("completed", []))
        pending_states = [s for s in STATES_AND_UTS if s not in completed_set]
        any_partial = any(os.path.exists(_xlsx_path_for(s)) for s in pending_states)
        if not any_partial:
            cp["runs_without_progress"] = cp.get("runs_without_progress", 0) + 1
            log.warning(
                f"No progress this run — stall counter now "
                f"{cp['runs_without_progress']}/{MAX_RUNS_WITHOUT_PROGRESS}."
            )
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
# State                    Est. records    Pages     Time
# ──────────────────────── ─────────────── ───────   ────────
# Uttar Pradesh              ~10,000,000   10,000   ~12.5 hrs  (3 runs)
# Maharashtra                 ~7,000,000    7,000   ~8.75 hrs  (2 runs)
# Gujarat                     ~5,000,000    5,000   ~6.25 hrs  (2 runs)
# Rajasthan                   ~4,000,000    4,000   ~5.0  hrs  (1 run)
# Tamil Nadu                  ~4,000,000    4,000   ~5.0  hrs  (1 run)
# West Bengal                 ~3,500,000    3,500   ~4.4  hrs  (1 run)
# Madhya Pradesh              ~3,000,000    3,000   ~3.75 hrs  (1 run)
# Karnataka                   ~3,000,000    3,000   ~3.75 hrs  (1 run)
# Andhra Pradesh              ~2,500,000    2,500   ~3.1  hrs  (1 run)
# Bihar                       ~2,500,000    2,500   ~3.1  hrs  (1 run)
# ... (26 smaller states)    ~15,000,000   15,000   ~18.75 hrs (~4 runs)
# ──────────────────────── ─────────────── ───────   ────────
# TOTAL                      ~62,500,000   62,500   ~78 hrs (~13-15 runs)
# Partial xlsx saved every run — no data ever discarded on deadline.
