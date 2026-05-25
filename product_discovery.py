#!/usr/bin/env python3
"""
Product Discovery Script
========================
Scrapes trending/bestselling products from Amazon India and Flipkart,
compares against existing products in data/Demand_Excel_Filled.xlsx,
reads supplier JSONs from data/suppliers/ to determine which states
have meaningful supplier presence for each product's NIC code,
and outputs data/new_products_suggestions.json for your review.

State allocation logic (tiered by supplier count):
  - Tier 1 (top 20% of states by supplier count) → included
  - Tier 2 (next 30%)                             → included
  - Tier 3 (bottom 50%)                           → excluded
  - Floor guarantee: minimum 3 states always included

After running:
  1. Open data/new_products_suggestions.json
  2. Fill in "Search Term" for products you want to keep
  3. Delete rows you don't want
  4. Upload back to GitHub (data/ folder)
  5. Run product_updater.py

Run schedule: Quarterly via GitHub Actions (after msme_supplier_fetcher.py).
Can also be run manually: python product_discovery.py

Requirements:
    pip install requests pandas openpyxl beautifulsoup4 lxml
"""

import requests
import pandas as pd
import json
import os
import time
import logging
import random
from bs4 import BeautifulSoup
from datetime import datetime
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────────────────────
DEMAND_FILE    = "data/Demand_Excel_Filled.xlsx"
NIC_CODES_FILE = "data/Key_NIC_Codes_List.xlsx"
SUPPLIERS_DIR  = "data/suppliers"
OUTPUT_FILE    = "data/new_products_suggestions.json"

MAX_PER_CATEGORY = 10
MIN_FLOOR_STATES = 3   # guarantee at least this many states per product

# Randomized delay ranges (in seconds)
MIN_SCRAPE_DELAY   = 4.5   # minimum delay between individual scrapes
MAX_SCRAPE_DELAY   = 8.7   # maximum delay between individual scrapes
MIN_CATEGORY_DELAY = 10.0  # minimum delay between categories
MAX_CATEGORY_DELAY = 15.5  # maximum delay between categories

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

def get_headers():
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept-Language": "en-IN,en;q=0.9",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def random_scrape_delay():
    """Sleep for a randomized duration between individual scrapes"""
    delay = random.uniform(MIN_SCRAPE_DELAY, MAX_SCRAPE_DELAY)
    log.info(f"💤 Waiting {delay:.2f}s before next scrape...")
    time.sleep(delay)


def random_category_delay():
    """Sleep for a randomized duration between categories"""
    delay = random.uniform(MIN_CATEGORY_DELAY, MAX_CATEGORY_DELAY)
    log.info(f"💤 Category complete. Waiting {delay:.2f}s before next category...")
    time.sleep(delay)


# ── LOAD REFERENCE DATA ───────────────────────────────────────────────────────
def load_existing_products() -> set:
    df       = pd.read_excel(DEMAND_FILE)
    prod_col = next(c for c in df.columns if 'product' in c.lower())
    return set(df[prod_col].str.lower().str.strip().tolist())


def load_nic_reference() -> tuple:
    nic_df    = pd.read_excel(NIC_CODES_FILE)
    demand_df = pd.read_excel(DEMAND_FILE)

    code_col  = next(c for c in nic_df.columns if 'nic' in c.lower() and 'code' in c.lower())
    desc_col  = next(c for c in nic_df.columns if 'desc' in c.lower())
    nic_col_d = next(c for c in demand_df.columns if 'nic' in c.lower())
    cat_col_d = next(c for c in demand_df.columns if 'cat' in c.lower())

    nic_df[code_col]     = nic_df[code_col].astype(str).str.strip()
    demand_df[nic_col_d] = demand_df[nic_col_d].astype(str).str.strip()

    nic_lookup = dict(zip(nic_df[code_col], nic_df[desc_col]))
    cat_lookup = (
        demand_df.groupby(nic_col_d)[cat_col_d]
        .agg(lambda x: x.mode()[0])
        .to_dict()
    )
    log.info(f"Loaded {len(nic_lookup)} NIC codes, {len(cat_lookup)} category mappings")
    return nic_lookup, cat_lookup


def load_categories_and_keywords(nic_lookup: dict, cat_lookup: dict) -> dict:
    demand_df  = pd.read_excel(DEMAND_FILE)
    nic_col_d  = next(c for c in demand_df.columns if 'nic' in c.lower())
    cat_col_d  = next(c for c in demand_df.columns if 'cat' in c.lower())
    prod_col_d = next(c for c in demand_df.columns if 'product' in c.lower())
    demand_df[nic_col_d] = demand_df[nic_col_d].astype(str).str.strip()
    nic_to_cat = dict(zip(demand_df[nic_col_d], demand_df[cat_col_d]))

    cat_data = {}
    for nic_code, nic_desc in nic_lookup.items():
        category = nic_to_cat.get(nic_code, nic_desc)
        keywords = _keywords_from_description(nic_desc)
        if category not in cat_data:
            cat_data[category] = {"nic_codes": [], "keywords": set()}
        cat_data[category]["nic_codes"].append(nic_code)
        cat_data[category]["keywords"].update(keywords)

    for _, row in demand_df.iterrows():
        cat     = row[cat_col_d]
        product = str(row[prod_col_d]).strip()
        if cat in cat_data:
            cat_data[cat]["keywords"].add(product.lower())

    return cat_data


def _keywords_from_description(desc: str) -> list:
    stop = {"of", "and", "or", "the", "in", "via", "other", "n.e.c",
            "not", "stores", "stalls", "markets", "retail", "sale",
            "wholesale", "manufacture", "articles", "related"}
    words    = desc.lower().replace(",", " ").replace(".", " ").split()
    keywords = [w for w in words if w not in stop and len(w) > 3]
    keywords.append(desc.lower())
    return keywords


# ── SUPPLIER STATE ANALYSIS ───────────────────────────────────────────────────
def load_supplier_counts_by_nic() -> dict:
    """
    Reads all supplier JSON files from data/suppliers/ and builds:
    {nic_code: {state: count}} — supplier count per state per NIC code.
    """
    nic_state_counts = defaultdict(lambda: defaultdict(int))

    if not os.path.exists(SUPPLIERS_DIR):
        log.warning(f"Suppliers directory not found: {SUPPLIERS_DIR}")
        log.warning("State allocation will use floor guarantee only.")
        return {}

    files = [f for f in os.listdir(SUPPLIERS_DIR) if f.endswith('.json')]
    if not files:
        log.warning("No supplier JSON files found. Run msme_supplier_fetcher.py first.")
        return {}

    log.info(f"Reading {len(files)} supplier state files...")
    for fname in files:
        path = os.path.join(SUPPLIERS_DIR, fname)
        try:
            with open(path, encoding="utf-8") as f:
                records = json.load(f)
            for record in records:
                nic_code = str(record.get("NIC_Code", "")).strip()
                state    = str(record.get("State", "")).strip().title()
                if nic_code and state:
                    nic_state_counts[nic_code][state] += 1
        except Exception as e:
            log.warning(f"Could not read {fname}: {e}")

    log.info(f"Loaded supplier counts for {len(nic_state_counts)} NIC codes")
    return dict(nic_state_counts)


def get_states_for_product(nic_code: str, nic_state_counts: dict) -> list:
    """
    Given a NIC code, returns a tiered list of states to add the product to.

    Tiering logic:
      - Rank all states by supplier count for this NIC code
      - Tier 1 = top 20% of states  → include
      - Tier 2 = next 30% of states → include
      - Tier 3 = bottom 50%         → exclude
      - Floor: always include at least MIN_FLOOR_STATES states

    Returns: list of dicts [{state, supplier_count, tier}]
    """
    state_counts = nic_state_counts.get(nic_code, {})

    if not state_counts:
        # No supplier data for this NIC code — return empty
        # product_updater will skip this product
        log.warning(f"No supplier data for NIC {nic_code} — product will be skipped")
        return []

    # Sort states by supplier count descending
    sorted_states = sorted(state_counts.items(), key=lambda x: x[1], reverse=True)
    total_states  = len(sorted_states)

    # Calculate tier cutoffs
    tier1_cutoff = max(1, int(total_states * 0.20))
    tier2_cutoff = max(2, int(total_states * 0.50))  # top 20% + next 30%

    selected = []
    for i, (state, count) in enumerate(sorted_states):
        if i < tier1_cutoff:
            tier = 1
        elif i < tier2_cutoff:
            tier = 2
        else:
            tier = 3  # excluded

        if tier <= 2:
            selected.append({
                "state":           state,
                "supplier_count":  count,
                "tier":            tier,
            })

    # Floor guarantee — always include at least MIN_FLOOR_STATES
    if len(selected) < MIN_FLOOR_STATES:
        already = {s["state"] for s in selected}
        for state, count in sorted_states:
            if len(selected) >= MIN_FLOOR_STATES:
                break
            if state not in already:
                selected.append({
                    "state":          state,
                    "supplier_count": count,
                    "tier":           3,  # forced inclusion
                })

    return selected


# ── SCRAPING ──────────────────────────────────────────────────────────────────
def scrape_amazon(search_term: str) -> list:
    url = f"https://www.amazon.in/s?k={requests.utils.quote(search_term)}&s=review-rank"
    try:
        resp = requests.get(url, headers=get_headers(), timeout=20)
        if resp.status_code != 200:
            log.warning(f"Amazon {resp.status_code} for '{search_term}'")
            return []
        soup     = BeautifulSoup(resp.text, "lxml")
        products = []
        for tag in soup.select("span.a-text-normal"):
            text = tag.get_text(strip=True)
            if 10 < len(text) < 120:
                products.append(text)
        return products[:15]
    except Exception as e:
        log.warning(f"Amazon error for '{search_term}': {e}")
        return []


def scrape_flipkart(search_term: str) -> list:
    url = f"https://www.flipkart.com/search?q={requests.utils.quote(search_term)}&sort=popularity"
    try:
        resp = requests.get(url, headers=get_headers(), timeout=20)
        if resp.status_code != 200:
            log.warning(f"Flipkart {resp.status_code} for '{search_term}'")
            return []
        soup     = BeautifulSoup(resp.text, "lxml")
        products = []
        for tag in soup.select("div._4rR01T, a.s1Q9rs, div.KzDlHZ"):
            text = tag.get_text(strip=True)
            if 10 < len(text) < 120:
                products.append(text)
        return products[:15]
    except Exception as e:
        log.warning(f"Flipkart error for '{search_term}': {e}")
        return []


# ── DISCOVERY ─────────────────────────────────────────────────────────────────
def discover_new_products(cat_data: dict, existing: set,
                          nic_state_counts: dict, cat_lookup: dict) -> list:
    suggestions = []
    total_categories = len(cat_data)
    category_num = 0

    for category, data in cat_data.items():
        category_num += 1
        nic_codes = data["nic_codes"]
        keywords  = list(data["keywords"])[:3]
        log.info(f"\n[{category_num}/{total_categories}] Searching: {category} ({len(nic_codes)} NIC codes)")
        found = []

        for keyword in keywords:
            log.info(f"  '{keyword}'")
            amazon_products   = scrape_amazon(keyword)
            random_scrape_delay()  # Randomized delay after Amazon scrape
            
            flipkart_products = scrape_flipkart(keyword)
            random_scrape_delay()  # Randomized delay after Flipkart scrape

            for product in amazon_products + flipkart_products:
                p_lower = product.lower().strip()
                if any(e in p_lower or p_lower in e for e in existing):
                    continue
                if any(s["Product"].lower() == p_lower for s in found):
                    continue

                # Use the primary NIC code for this category
                nic_code = nic_codes[0]

                # Get tiered state allocation for this NIC code
                state_allocation = get_states_for_product(nic_code, nic_state_counts)

                found.append({
                    "Product":          product,
                    "Category":         category,
                    "NIC_Code":         nic_code,
                    "Search Term":      "",   # ← you fill this in
                    "Source":           "Amazon" if product in amazon_products else "Flipkart",
                    "Keyword Used":     keyword,
                    "State_Allocation": state_allocation,
                    "States_Count":     len(state_allocation),
                })

        suggestions.extend(found[:MAX_PER_CATEGORY])
        log.info(f"  Found {len(found)} new products for {category}")
        
        # Add delay between categories (except after the last one)
        if category_num < total_categories:
            random_category_delay()

    return suggestions


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Product Discovery — Quarterly Run")
    log.info(f"Date: {datetime.now().strftime('%Y-%m-%d')}")
    log.info("=" * 60)

    existing              = load_existing_products()
    nic_lookup, cat_lookup = load_nic_reference()
    cat_data              = load_categories_and_keywords(nic_lookup, cat_lookup)
    nic_state_counts      = load_supplier_counts_by_nic()

    log.info(f"Existing products : {len(existing)}")
    log.info(f"Categories        : {len(cat_data)}")
    log.info(f"NIC codes with supplier data: {len(nic_state_counts)}")

    suggestions = discover_new_products(cat_data, existing, nic_state_counts, cat_lookup)

    # Summary stats
    total_rows = sum(s["States_Count"] for s in suggestions)

    os.makedirs("data", exist_ok=True)
    output = {
        "generated_date": datetime.now().strftime("%Y-%m-%d"),
        "instructions": (
            "Each suggestion includes a State_Allocation list showing which states "
            "have meaningful supplier presence for this product's NIC code (tiered by count). "
            "1. Fill in 'Search Term' for products you want to keep. "
            "2. Delete rows you don't want. "
            "3. Upload back to GitHub (data/ folder). "
            "4. Run product_updater.py — it will add one row per allocated state."
        ),
        "total_suggestions": len(suggestions),
        "total_rows_if_all_approved": total_rows,
        "suggestions": suggestions,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info("\n" + "=" * 60)
    log.info(f"Total suggestions          : {len(suggestions)}")
    log.info(f"Total rows if all approved : {total_rows}")
    log.info(f"Avg states per product     : {total_rows // max(len(suggestions), 1)}")
    log.info(f"Saved to                   : {OUTPUT_FILE}")
    log.info("Next: fill in Search Terms, delete unwanted rows,")
    log.info("      upload back to GitHub, then run product_updater.py")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
