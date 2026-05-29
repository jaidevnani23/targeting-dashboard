#!/usr/bin/env python3
"""
Product Updater
===============
Reads approved products from data/new_products_suggestions.json
and appends them to data/Demand_Excel_Filled.xlsx.

Unlike the old version (which added every product to all 36 states),
this version adds each product only to the states allocated by
product_discovery.py based on supplier concentration for that NIC code.

State allocation per product:
  - Tier 1 states (top 20% by supplier count) → always included
  - Tier 2 states (next 30%)                  → included
  - Floor guarantee                            → minimum 3 states always

Run MANUALLY after reviewing new_products_suggestions.json, OR pass --yes
to skip the confirmation prompt (used automatically by GitHub Actions CI).

Usage:
    python product_updater.py           # interactive — asks for confirmation
    python product_updater.py --yes     # non-interactive — proceeds automatically

Requirements:
    pip install pandas openpyxl
"""

import argparse
import json
import logging
import os
import shutil
from datetime import datetime

import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
DEMAND_FILE      = "data/Demand_Excel_Filled.xlsx"
SUGGESTIONS_FILE = "data/new_products_suggestions.json"
BACKUP_DIR       = "data/backups"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ── LOAD ──────────────────────────────────────────────────────────────────────
def load_suggestions() -> list:
    if not os.path.exists(SUGGESTIONS_FILE):
        raise FileNotFoundError(
            f"{SUGGESTIONS_FILE} not found. Run product_discovery.py first."
        )
    with open(SUGGESTIONS_FILE, encoding="utf-8") as f:
        data = json.load(f)

    suggestions = data.get("suggestions", [])

    # Only keep rows where Search Term has been filled in
    approved = [s for s in suggestions if s.get("Search Term", "").strip()]
    skipped  = len(suggestions) - len(approved)

    log.info(f"Total suggestions          : {len(suggestions)}")
    log.info(f"Approved (with Search Term): {len(approved)}")
    log.info(f"Skipped (no Search Term)   : {skipped}")

    # Warn about products with no state allocation
    no_states = [s for s in approved if not s.get("State_Allocation")]
    if no_states:
        log.warning(
            f"  {len(no_states)} approved product(s) have no state allocation "
            f"(supplier data missing for their NIC code) — they will be skipped."
        )

    if not approved:
        raise ValueError(
            f"No approved suggestions found. Open {SUGGESTIONS_FILE}, "
            f"fill in 'Search Term' for products you want, then re-run."
        )
    return approved


def load_demand_excel() -> pd.DataFrame:
    return pd.read_excel(DEMAND_FILE)


# ── BACKUP ────────────────────────────────────────────────────────────────────
def backup_demand_excel():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"Demand_Excel_Filled_{ts}.xlsx")
    shutil.copy2(DEMAND_FILE, backup_path)
    log.info(f"Backup saved: {backup_path}")


# ── BUILD NEW ROWS ────────────────────────────────────────────────────────────
def build_new_rows(approved: list, existing_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each approved suggestion, creates one row per allocated state.
    Uses the NIC code from the suggestion (strictly — no cross-state switching).
    Skips products with no state allocation or that already exist.
    """
    state_col  = next(c for c in existing_df.columns if 'state'   in c.lower())
    prod_col   = next(c for c in existing_df.columns if 'product' in c.lower())
    nic_col    = next(c for c in existing_df.columns if 'nic'     in c.lower())
    cat_col    = next(c for c in existing_df.columns if 'cat'     in c.lower())
    search_col = next(c for c in existing_df.columns if 'search'  in c.lower())

    existing_pairs = set(zip(
        existing_df[state_col].str.lower().str.strip(),
        existing_df[prod_col].str.lower().str.strip()
    ))

    new_rows          = []
    skipped_dupes     = 0
    skipped_no_states = 0

    for suggestion in approved:
        product          = suggestion["Product"].strip()
        category         = suggestion["Category"].strip()
        nic_code         = str(suggestion["NIC_Code"]).strip()
        search_term      = suggestion["Search Term"].strip()
        state_allocation = suggestion.get("State_Allocation", [])

        if not state_allocation:
            log.warning(
                f"Skipping '{product}' — no state allocation "
                f"(NIC {nic_code} has no supplier data)"
            )
            skipped_no_states += 1
            continue

        for state_info in state_allocation:
            state = state_info["state"].strip()
            pair  = (state.lower(), product.lower())

            if pair in existing_pairs:
                skipped_dupes += 1
                continue

            new_rows.append({
                state_col:  state,
                prod_col:   product,
                nic_col:    nic_code,
                cat_col:    category,
                search_col: search_term,
            })
            existing_pairs.add(pair)

    log.info(f"New rows to add          : {len(new_rows)}")
    log.info(f"Duplicates skipped       : {skipped_dupes}")
    log.info(f"Skipped (no state data)  : {skipped_no_states}")
    return pd.DataFrame(new_rows)


# ── SAVE ──────────────────────────────────────────────────────────────────────
def save_updated_excel(existing_df: pd.DataFrame, new_rows_df: pd.DataFrame):
    combined = pd.concat([existing_df, new_rows_df], ignore_index=True)
    combined.to_excel(DEMAND_FILE, index=False)
    log.info(f"Excel saved : {DEMAND_FILE}")
    log.info(f"Total rows  : {len(combined)} (was {len(existing_df)})")


def archive_suggestions():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(BACKUP_DIR, f"suggestions_processed_{ts}.json")
    shutil.move(SUGGESTIONS_FILE, archive_path)
    log.info(f"Suggestions archived: {archive_path}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Product Updater")
    parser.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip confirmation prompt (used by GitHub Actions CI)",
    )
    args = parser.parse_args()

    # Also auto-skip confirmation when running in CI (GitHub Actions sets CI=true)
    auto_confirm = args.yes or os.environ.get("CI", "").lower() == "true"

    log.info("=" * 60)
    log.info("Product Updater — %s", "CI Run" if auto_confirm else "Manual Run")
    log.info(f"Date: {datetime.now().strftime('%Y-%m-%d')}")
    log.info("=" * 60)

    approved    = load_suggestions()
    existing_df = load_demand_excel()

    log.info(f"Current Excel rows : {len(existing_df)}")

    backup_demand_excel()
    new_rows_df = build_new_rows(approved, existing_df)

    if new_rows_df.empty:
        log.info(
            "No new rows to add — all approved products already exist "
            "or have no state data."
        )
        return

    # ── Preview ───────────────────────────────────────────────────────────────
    print(f"\nAbout to add {len(new_rows_df)} rows "
          f"across {len(approved)} approved products:\n")
    for suggestion in approved:
        product          = suggestion["Product"]
        nic_code         = suggestion["NIC_Code"]
        category         = suggestion["Category"]
        state_allocation = suggestion.get("State_Allocation", [])
        if not state_allocation:
            print(f"  x {product} — skipped (no supplier data for NIC {nic_code})")
            continue
        tier1 = sum(1 for s in state_allocation if s["tier"] == 1)
        tier2 = sum(1 for s in state_allocation if s["tier"] == 2)
        tier3 = sum(1 for s in state_allocation if s["tier"] == 3)
        print(f"  + {product}")
        print(f"      NIC: {nic_code} | Category: {category}")
        print(f"      States: {len(state_allocation)} total  "
              f"(Tier 1: {tier1}  Tier 2: {tier2}  Floor: {tier3})")
        top_states = [s["state"] for s in state_allocation[:5]]
        print(f"      Top states: {', '.join(top_states)}" +
              (f"... +{len(state_allocation)-5} more"
               if len(state_allocation) > 5 else ""))
        print()

    # ── Confirm ───────────────────────────────────────────────────────────────
    if auto_confirm:
        log.info("CI mode — skipping confirmation prompt, proceeding automatically.")
    else:
        confirm = input("Proceed? (yes/no): ").strip().lower()
        if confirm not in ("yes", "y"):
            log.info("Cancelled. No changes made.")
            return

    save_updated_excel(existing_df, new_rows_df)
    archive_suggestions()

    log.info("\n" + "=" * 60)
    log.info("Done! Next steps:")
    log.info("1. Demand_Excel_Filled.xlsx updated in data/ folder")
    log.info("2. Trends scraper picks up new products on its next monthly run")
    log.info("3. To run trends immediately:")
    log.info("   GitHub -> Actions -> Update Trends Data -> Run workflow")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
