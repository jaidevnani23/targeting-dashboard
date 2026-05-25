#!/usr/bin/env python3
"""
Google Trends Scraper → Dashboard Automation
Scrapes Google Trends data and automatically generates dashboard-ready JSON.
Supports resume — partial progress is saved after every product.
"""

import time
import random
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import openpyxl
from pathlib import Path

# ── URLLIB3 COMPATIBILITY PATCH ───────────────────────────────────────────────
import urllib3
from urllib3.util.retry import Retry

_original_retry_init = Retry.__init__

def _patched_retry_init(self, *args, **kwargs):
    if 'method_whitelist' in kwargs:
        kwargs['allowed_methods'] = kwargs.pop('method_whitelist')
    _original_retry_init(self, *args, **kwargs)

Retry.__init__ = _patched_retry_init

if not hasattr(Retry, 'DEFAULT_METHOD_WHITELIST'):
    Retry.DEFAULT_METHOD_WHITELIST = Retry.DEFAULT_ALLOWED_METHODS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from pytrends.request import TrendReq

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()

BRIGHTDATA_USERNAME = os.getenv("BRIGHTDATA_USERNAME")
BRIGHTDATA_PASSWORD = os.getenv("BRIGHTDATA_PASSWORD")
BRIGHTDATA_HOST     = os.getenv("BRIGHTDATA_HOST")
BRIGHTDATA_PORT     = os.getenv("BRIGHTDATA_PORT")

INPUT_EXCEL   = "data/Demand_Excel_Filled.xlsx"
OUTPUT_EXCEL  = "data/Demand_Excel_With_Trends.xlsx"
OUTPUT_JSON   = "data/dashboard_trends_data.json"
PROGRESS_FILE = "data/scraping_progress.json"

PRODUCTS_PER_BATCH          = 10
TOP_MONTHS_COUNT            = 20
MIN_SECONDS_BETWEEN_TERMS   = 15.0
MAX_SECONDS_BETWEEN_TERMS   = 60.0
MIN_MINUTES_BETWEEN_BATCHES = 18
MAX_MINUTES_BETWEEN_BATCHES = 23
TIMEFRAME                   = "today 5-y"
GEO                         = "IN"

MONTH_TO_INDEX = {
    'January': 0, 'February': 1, 'March': 2, 'April': 3,
    'May': 4, 'June': 5, 'July': 6, 'August': 7,
    'September': 8, 'October': 9, 'November': 10, 'December': 11
}


class DashboardTrendsScraper:
    def __init__(self):
        proxy_url = f"http://{BRIGHTDATA_USERNAME}:{BRIGHTDATA_PASSWORD}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"

        print(f"🌐 Initializing with Bright Data proxy...")
        print(f"   Host: {BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}")
        print(f"   User: {BRIGHTDATA_USERNAME}")
        print(f"   Geography: India (IN)")

        self.pytrends = TrendReq(
            hl='en-IN', tz=330, timeout=(15, 30),
            proxies=[proxy_url], retries=2, backoff_factor=0.5,
            requests_args={'verify': False}
        )

        os.makedirs("data", exist_ok=True)
        self.progress     = self.load_progress()
        self.dashboard_data = []
        print(f"✅ Proxy configured successfully!\n")

    # ── PROGRESS ──────────────────────────────────────────────────────────────
    def load_progress(self):
        if Path(PROGRESS_FILE).exists():
            with open(PROGRESS_FILE, 'r') as f:
                p = json.load(f)
                p.setdefault('failed_attempts', {})
                p.setdefault('total_batches_completed', 0)
                p.setdefault('last_batch_time', None)
                return p
        return {'scraped_indices': [], 'failed_attempts': {},
                'total_batches_completed': 0, 'last_batch_time': None}

    def save_progress(self):
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(self.progress, f, indent=2)

    # ── KEY FIX: RELOAD PREVIOUS RESULTS FROM EXCEL ON RESUME ────────────────
    def reload_dashboard_data_from_excel(self, df):
        """
        On resume, rebuild dashboard_data from the partially completed Excel.
        This ensures the JSON always contains ALL scraped products, not just
        the ones from the current run.
        """
        if not Path(OUTPUT_EXCEL).exists():
            return

        scraped_indices = set(self.progress['scraped_indices'])
        if not scraped_indices:
            return

        print(f"🔄 Reloading {len(scraped_indices)} previously scraped products from Excel...")
        reloaded = 0

        for idx in sorted(scraped_indices):
            if idx >= len(df):
                continue
            row = df.iloc[idx]

            # Rebuild monthly series from Top_N_Month / Top_N_Value columns
            top_months = {}
            for rank in range(1, TOP_MONTHS_COUNT + 1):
                month_col = f'Top_{rank}_Month'
                value_col = f'Top_{rank}_Value'
                if month_col in df.columns and value_col in df.columns:
                    m = row.get(month_col)
                    v = row.get(value_col)
                    if pd.notna(m) and pd.notna(v):
                        top_months[m] = float(v)

            if not top_months:
                continue

            product = row.get('Product', 'Unknown')
            state   = row.get('State', 'All India')

            for month_str, value in top_months.items():
                try:
                    dt = datetime.strptime(month_str, '%Y-%m')
                    month_name  = dt.strftime('%B')
                    month_index = MONTH_TO_INDEX.get(month_name)
                    if month_index is None:
                        continue
                    demand_score = min(4, int(value / 20))
                    self.dashboard_data.append({
                        "product":     product,
                        "state":       state,
                        "month_index": month_index,
                        "new_score":   demand_score,
                        "trend_value": round(value, 2),
                        "month_name":  month_name,
                    })
                except (ValueError, TypeError):
                    continue

            reloaded += 1

        print(f"✅ Reloaded {reloaded} products ({len(self.dashboard_data)} demand adjustments) from previous run")
        # export is called from run_continuous after reload returns (with df)

    # ── FETCH ─────────────────────────────────────────────────────────────────
    def get_highly_random_delay(self, min_s, max_s):
        return round(random.uniform(min_s, max_s), 5)

    def parse_search_term(self, search_term):
        if not search_term or pd.isna(search_term):
            return []
        return [t.strip() for t in str(search_term).split('+') if t.strip()]

    def fetch_trends_for_single_term(self, term, retries=3):
        for attempt in range(retries):
            try:
                print(f"      → Querying: '{term}'")
                self.pytrends.build_payload([term], cat=0, timeframe=TIMEFRAME, geo=GEO, gprop='')
                interest_df = self.pytrends.interest_over_time()
                if interest_df is not None and not interest_df.empty and term in interest_df.columns:
                    print(f"      ✓ Got {len(interest_df)} months (avg: {interest_df[term].mean():.1f})")
                    return interest_df[term]
                print(f"      ⚠ No data for '{term}'")
                return None
            except Exception as e:
                print(f"      ✗ Error: {str(e)}")
                if attempt < retries - 1:
                    wait = self.get_highly_random_delay(10, 20)
                    print(f"      ⏳ Retry in {wait:.5f}s...")
                    time.sleep(wait)
                else:
                    print(f"      ⛔ Failed after {retries} attempts")
        return None

    def fetch_trends(self, search_term):
        terms = self.parse_search_term(search_term)
        if not terms:
            print("  ⚠️  Empty search term")
            return None, None, None

        print(f"  📊 Terms: {terms} ({len(terms)} term(s))")
        all_data = []

        for i, term in enumerate(terms, 1):
            print(f"    [{i}/{len(terms)}] {term}")
            data = self.fetch_trends_for_single_term(term)
            if data is not None:
                all_data.append(data)
            if i < len(terms):
                delay = self.get_highly_random_delay(MIN_SECONDS_BETWEEN_TERMS, MAX_SECONDS_BETWEEN_TERMS)
                print(f"      ⏳ Wait {delay:.5f}s before next term...")
                time.sleep(delay)

        if not all_data:
            print("  ❌ No data retrieved")
            return None, None, None

        combined_series = sum(all_data) / len(all_data)
        avg_interest    = combined_series.mean()
        split           = len(combined_series) // 3
        early_avg       = combined_series.iloc[:split].mean()
        recent_avg      = combined_series.iloc[-split:].mean()

        if recent_avg > early_avg * 1.2:
            trend_direction = "📈 Rising"
        elif recent_avg < early_avg * 0.8:
            trend_direction = "📉 Declining"
        else:
            trend_direction = "➡️ Stable"

        return round(avg_interest, 2), trend_direction, combined_series

    # ── DASHBOARD FORMAT ──────────────────────────────────────────────────────
    def convert_to_dashboard_format(self, product, state, category_group, monthly_series):
        if monthly_series is None or monthly_series.empty:
            return []
        adjustments = []
        for date, value in monthly_series.nlargest(TOP_MONTHS_COUNT).items():
            month_name  = date.strftime('%B')
            month_index = MONTH_TO_INDEX.get(month_name)
            if month_index is not None:
                adjustments.append({
                    "product":     product,
                    "state":       state,
                    "month_index": month_index,
                    "new_score":   min(4, int(value / 20)),
                    "trend_value": round(float(value), 2),
                    "month_name":  month_name,
                })
        return adjustments

    def get_remaining_indices(self, total_rows):
        scraped = set(self.progress['scraped_indices'])
        return list(set(range(total_rows)) - scraped)

    def initialize_top_months_columns(self, df):
        for i in range(1, TOP_MONTHS_COUNT + 1):
            for col in [f'Top_{i}_Month', f'Top_{i}_Value']:
                if col not in df.columns:
                    df[col] = None
        print(f"  📅 Initialized columns for top {TOP_MONTHS_COUNT} months")

    def store_top_months(self, df, idx, monthly_series):
        if monthly_series is None or monthly_series.empty:
            return
        top_months = monthly_series.nlargest(TOP_MONTHS_COUNT)
        for rank, (date, value) in enumerate(top_months.items(), 1):
            df.at[idx, f'Top_{rank}_Month'] = date.strftime('%Y-%m')
            df.at[idx, f'Top_{rank}_Value'] = round(float(value), 2)
        print(f"  📊 Top month: {top_months.index[0].strftime('%Y-%m')} (value: {top_months.iloc[0]:.2f})")

    def export_dashboard_json(self, df=None):
        """
        Exports dashboard_trends_data.json as a flat array of rows —
        one row per State+Product, matching the Excel column structure exactly.
        This is the format the dashboard reads directly.
        """
        if df is None:
            print("\n⚠️  export_dashboard_json called without df — skipping")
            return

        scraped_df = df[df['Trend_Direction'].notna() & (df['Trend_Direction'] != '')].copy()
        if scraped_df.empty:
            print("\n⚠️  No scraped rows to export yet")
            return

        scraped_df = scraped_df.fillna('')

        if 'NIC Code' in scraped_df.columns:
            scraped_df['NIC Code'] = scraped_df['NIC Code'].apply(
                lambda x: str(int(float(x))) if x not in ('', None) else ''
            )

        rows = scraped_df.to_dict(orient='records')

        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)

        unique = scraped_df['Product'].nunique() if 'Product' in scraped_df.columns else len(rows)
        print(f"\n✅ Dashboard JSON exported: {OUTPUT_JSON}")
        print(f"   📊 {len(rows)} rows | 📦 {unique} unique products")

    # ── BATCH RUNNER ──────────────────────────────────────────────────────────
    def run_single_batch(self, df):
        remaining = self.get_remaining_indices(len(df))
        if not remaining:
            return False, "ALL_COMPLETE" if not self.progress['failed_attempts'] else "RETRYING_FAILED"

        if 'Top_1_Month' not in df.columns:
            self.initialize_top_months_columns(df)

        failed_indices  = [int(i) for i in self.progress['failed_attempts']]
        failed_to_retry = [i for i in failed_indices if i in remaining]

        if failed_to_retry:
            n = min(len(failed_to_retry), PRODUCTS_PER_BATCH)
            batch = failed_to_retry[:n]
            extra = [i for i in remaining if i not in failed_to_retry]
            random.shuffle(extra)
            batch.extend(extra[:PRODUCTS_PER_BATCH - n])
            batch = sorted(batch)
            print(f"📍 {n} retry + {PRODUCTS_PER_BATCH - n} new")
        else:
            random.shuffle(remaining)
            batch = sorted(remaining[:PRODUCTS_PER_BATCH])
            print(f"🆕 {len(batch)} new products")

        batch_num     = self.progress['total_batches_completed'] + 1
        success_count = 0
        start_time    = time.time()
        print(f"Batch #{batch_num}")

        for i, idx in enumerate(batch):
            row           = df.iloc[idx]
            search_term   = row.get('Search Term', '')
            product       = row.get('Product', 'Unknown')
            state         = row.get('State', 'All India')
            category_group= row.get('Category Group', 'general')

            print(f"\n{'='*70}")
            print(f"[{i+1}/{len(batch)}] Row {idx+1} - {product}")
            print(f"🔍 '{search_term}'")

            avg_interest, trend_direction, monthly_series = self.fetch_trends(search_term)

            df.at[idx, 'Avg_Interest_5Y'] = avg_interest
            df.at[idx, 'Trend_Direction'] = trend_direction
            df.at[idx, 'Last_Updated']    = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if monthly_series is not None:
                self.store_top_months(df, idx, monthly_series)
                adjustments = self.convert_to_dashboard_format(product, state, category_group, monthly_series)
                self.dashboard_data.extend(adjustments)
                print(f"  📊 Generated {len(adjustments)} dashboard adjustments")

            if avg_interest is not None:
                success_count += 1
                print(f"  ✅ Interest: {avg_interest} | {trend_direction}")
                self.progress['scraped_indices'].append(idx)
                self.progress['failed_attempts'].pop(str(idx), None)
            else:
                print(f"  ❌ Failed")
                self.progress['failed_attempts'].setdefault(str(idx), 0)
                self.progress['failed_attempts'][str(idx)] += 1

            # Save everything after every single product
            self.save_progress()
            df.to_excel(OUTPUT_EXCEL, index=False, engine='openpyxl')
            if avg_interest is not None:
                self.export_dashboard_json(df)

        self.progress['total_batches_completed'] += 1
        self.progress['last_batch_time'] = datetime.now().isoformat()
        self.save_progress()

        elapsed = time.time() - start_time
        scraped = len(self.progress['scraped_indices'])
        print(f"\n{'='*70}")
        print(f"✅ BATCH #{batch_num} COMPLETE — {success_count}/{len(batch)} success — {elapsed/60:.1f} min")
        print(f"📊 Total progress: {scraped}/{len(df)} ({scraped/len(df)*100:.1f}%)")
        print(f"{'='*70}")
        return True, None

    # ── MAIN LOOP ─────────────────────────────────────────────────────────────
    def run_continuous(self):
        print(f"""
╔═══════════════════════════════════════════════════════════════════╗
║   Google Trends → Dashboard Automation (Top {TOP_MONTHS_COUNT} Months)        ║
╚═══════════════════════════════════════════════════════════════════╝
📂 Input:        {INPUT_EXCEL}
📂 Output Excel: {OUTPUT_EXCEL}
📂 Output JSON:  {OUTPUT_JSON}
🔢 Batch size:   {PRODUCTS_PER_BATCH} products
⏱️  Term delays:  {MIN_SECONDS_BETWEEN_TERMS}-{MAX_SECONDS_BETWEEN_TERMS}s
⏱️  Batch delays: {MIN_MINUTES_BETWEEN_BATCHES}-{MAX_MINUTES_BETWEEN_BATCHES} minutes
""")
        print("📖 Loading Excel file...")
        try:
            if Path(OUTPUT_EXCEL).exists():
                df = pd.read_excel(OUTPUT_EXCEL)
                print(f"✅ Loaded existing output: {len(df)} rows")
            else:
                df = pd.read_excel(INPUT_EXCEL)
                print(f"✅ Loaded input file: {len(df)} rows")
                df['Avg_Interest_5Y'] = None
                df['Trend_Direction'] = None
                df['Last_Updated']    = None
        except FileNotFoundError:
            print(f"❌ Could not find '{INPUT_EXCEL}'")
            return

        # KEY: reload all previous results so JSON is always cumulative
        self.reload_dashboard_data_from_excel(df)
        self.export_dashboard_json(df)  # export immediately with reloaded data

        remaining = self.get_remaining_indices(len(df))
        scraped   = len(self.progress['scraped_indices'])
        failed    = len(self.progress['failed_attempts'])

        print(f"\n📊 Progress: {scraped} scraped | {failed} failed | {len(remaining)} remaining")
        print(f"   Batches completed: {self.progress['total_batches_completed']}")

        if not remaining and failed == 0:
            print(f"\n🎉 ALL DONE! {scraped}/{len(df)} products scraped!")
            self.export_dashboard_json(df)
            return

        print(f"\n🚀 Starting scraping...\n")

        while True:
            has_more, status = self.run_single_batch(df)

            if not has_more:
                if status == "ALL_COMPLETE":
                    print(f"\n🎉 ALL PRODUCTS SCRAPED! {scraped}/{len(df)}")
                    self.export_dashboard_json(df)
                break

            if len(df) - len(self.progress['scraped_indices']) == 0 and not self.progress['failed_attempts']:
                self.export_dashboard_json(df)
                break

            wait_min = random.randint(MIN_MINUTES_BETWEEN_BATCHES, MAX_MINUTES_BETWEEN_BATCHES)
            wait_sec = round(random.uniform(0, 59.99999), 5)
            total    = wait_min * 60 + wait_sec
            next_t   = datetime.now() + timedelta(seconds=total)
            remaining_now = len(df) - len(self.progress['scraped_indices'])

            print(f"\n⏳ WAITING {wait_min} min {wait_sec:.5f} sec until next batch")
            print(f"   Next batch at: {next_t.strftime('%I:%M:%S %p')}")
            print(f"   Remaining: {remaining_now} products\n{'='*70}\n")
            time.sleep(total)

        print(f"\n💾 Excel: {OUTPUT_EXCEL}")
        print(f"💾 JSON:  {OUTPUT_JSON}")


if __name__ == "__main__":
    try:
        scraper = DashboardTrendsScraper()
        scraper.run_continuous()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Progress saved. Run again to resume.")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
