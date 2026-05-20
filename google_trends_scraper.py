#!/usr/bin/env python3
"""
Google Trends Scraper → Dashboard Automation
Scrapes Google Trends data and automatically generates dashboard-ready JSON
"""

import time
import random
import pandas as pd
import json
from datetime import datetime, timedelta
import openpyxl
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════
#                   URLLIB3 COMPATIBILITY PATCH
# ═══════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════
#                           CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

INPUT_EXCEL = r"C:\Users\jaide\PycharmProjects\PythonProject\Demand_Excel_Filled.xlsx"
OUTPUT_EXCEL = r"C:\Users\jaide\PycharmProjects\PythonProject\Demand_Excel_With_Trends.xlsx"
OUTPUT_JSON = "dashboard_trends_data.json"  # Dashboard-ready JSON output
PROGRESS_FILE = "scraping_progress.json"

# Batch configuration
PRODUCTS_PER_BATCH = 10
TOP_MONTHS_COUNT = 20

# Timing configuration
MIN_SECONDS_BETWEEN_TERMS = 15.0
MAX_SECONDS_BETWEEN_TERMS = 60.0
MIN_MINUTES_BETWEEN_BATCHES = 18
MAX_MINUTES_BETWEEN_BATCHES = 23

# Google Trends settings
TIMEFRAME = "today 5-y"
GEO = "IN"

# ═══════════════════════════════════════════════════════════════════════
#                    BRIGHT DATA PROXY CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

from dotenv import load_dotenv
import os
load_dotenv()

BRIGHTDATA_USERNAME = os.getenv("BRIGHTDATA_USERNAME")
BRIGHTDATA_PASSWORD = os.getenv("BRIGHTDATA_PASSWORD")
BRIGHTDATA_HOST     = os.getenv("BRIGHTDATA_HOST")
BRIGHTDATA_PORT     = os.getenv("BRIGHTDATA_PORT")

# ═══════════════════════════════════════════════════════════════════════
#                    DASHBOARD DATA MAPPING
# ═══════════════════════════════════════════════════════════════════════

# Month name to index mapping (as used in your dashboard)
MONTH_TO_INDEX = {
    'January': 0, 'February': 1, 'March': 2, 'April': 3,
    'May': 4, 'June': 5, 'July': 6, 'August': 7,
    'September': 8, 'October': 9, 'November': 10, 'December': 11
}


class DashboardTrendsScraper:
    def __init__(self):
        """Initialize the scraper with Bright Data proxy support"""
        proxy_url = f"http://{BRIGHTDATA_USERNAME}:{BRIGHTDATA_PASSWORD}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"
        proxies = [proxy_url]

        print(f"🌐 Initializing with Bright Data proxy...")
        print(f"   Host: {BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}")
        print(f"   User: {BRIGHTDATA_USERNAME}")
        print(f"   Geography: India (IN)")

        self.pytrends = TrendReq(
            hl='en-IN',
            tz=330,
            timeout=(15, 30),
            proxies=proxies,
            retries=2,
            backoff_factor=0.5,
            requests_args={'verify': False}
        )

        self.progress = self.load_progress()
        self.dashboard_data = []  # Accumulate dashboard-ready data
        print(f"✅ Proxy configured successfully!\n")

    def load_progress(self):
        """Load previous scraping progress"""
        if Path(PROGRESS_FILE).exists():
            with open(PROGRESS_FILE, 'r') as f:
                progress = json.load(f)
                if 'failed_attempts' not in progress:
                    progress['failed_attempts'] = {}
                if 'total_batches_completed' not in progress:
                    progress['total_batches_completed'] = 0
                if 'last_batch_time' not in progress:
                    progress['last_batch_time'] = None
                return progress
        return {
            'scraped_indices': [],
            'failed_attempts': {},
            'total_batches_completed': 0,
            'last_batch_time': None
        }

    def save_progress(self):
        """Save current progress"""
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(self.progress, f, indent=2)

    def get_highly_random_delay(self, min_seconds, max_seconds):
        """Generate highly randomized delay with 5 decimal precision"""
        return round(random.uniform(min_seconds, max_seconds), 5)

    def parse_search_term(self, search_term):
        """Parse compound search terms (split by '+')"""
        if not search_term or pd.isna(search_term):
            return []
        terms = [term.strip() for term in str(search_term).split('+')]
        return [t for t in terms if t]

    def fetch_trends_for_single_term(self, term, retries=3):
        """Fetch Google Trends data for ONE specific term"""
        for attempt in range(retries):
            try:
                print(f"      → Querying: '{term}'")

                self.pytrends.build_payload(
                    [term],
                    cat=0,
                    timeframe=TIMEFRAME,
                    geo=GEO,
                    gprop=''
                )

                interest_df = self.pytrends.interest_over_time()

                if interest_df is not None and not interest_df.empty and term in interest_df.columns:
                    data_points = len(interest_df)
                    avg_value = interest_df[term].mean()
                    print(f"      ✓ Got {data_points} months (avg: {avg_value:.1f})")
                    return interest_df[term]
                else:
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

        return None

    def fetch_trends(self, search_term):
        """Fetch and aggregate Google Trends data for compound terms"""
        terms = self.parse_search_term(search_term)

        if not terms:
            print(f"  ⚠️  Empty search term")
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
            print(f"  ❌ No data retrieved")
            return None, None, None

        print(f"  ✅ Data from {len(all_data)}/{len(terms)} term(s)")

        # Aggregate data
        combined_series = sum(all_data) / len(all_data)
        avg_interest = combined_series.mean()

        # Trend direction
        split_point = len(combined_series) // 3
        early_avg = combined_series.iloc[:split_point].mean()
        recent_avg = combined_series.iloc[-split_point:].mean()

        if recent_avg > early_avg * 1.2:
            trend_direction = "📈 Rising"
        elif recent_avg < early_avg * 0.8:
            trend_direction = "📉 Declining"
        else:
            trend_direction = "➡️ Stable"

        return round(avg_interest, 2), trend_direction, combined_series

    def convert_to_dashboard_format(self, product, state, category_group, monthly_series):
        """
        Convert Google Trends data to dashboard format.

        Dashboard expects demand_adjustments with:
        - product, state, month_index (0-11), new_score (0-4 scale)
        """
        if monthly_series is None or monthly_series.empty:
            return []

        adjustments = []

        # Get top 20 months
        top_months = monthly_series.nlargest(TOP_MONTHS_COUNT)

        # Convert each top month to dashboard format
        for date, value in top_months.items():
            month_name = date.strftime('%B')  # Full month name
            month_index = MONTH_TO_INDEX.get(month_name)

            if month_index is not None:
                # Convert Google Trends value (0-100) to dashboard demand scale (0-4)
                # 0-20 = 0 (Minimal), 21-40 = 1 (Low), 41-60 = 2 (Moderate),
                # 61-80 = 3 (High), 81-100 = 4 (Peak)
                demand_score = min(4, int(value / 20))

                adjustment = {
                    "product": product,
                    "state": state,
                    "month_index": month_index,
                    "new_score": demand_score,
                    "trend_value": round(float(value), 2),
                    "month_name": month_name
                }
                adjustments.append(adjustment)

        return adjustments

    def get_remaining_indices(self, total_rows):
        """Get indices that haven't been scraped yet"""
        scraped = set(self.progress['scraped_indices'])
        all_indices = set(range(total_rows))
        remaining = list(all_indices - scraped)
        return remaining

    def initialize_top_months_columns(self, df):
        """Initialize columns for top 20 months"""
        for i in range(1, TOP_MONTHS_COUNT + 1):
            month_col = f'Top_{i}_Month'
            value_col = f'Top_{i}_Value'

            if month_col not in df.columns:
                df[month_col] = None
            if value_col not in df.columns:
                df[value_col] = None

        print(f"  📅 Initialized columns for top {TOP_MONTHS_COUNT} months")

    def store_top_months(self, df, idx, monthly_series):
        """Store the top N months with highest demand"""
        if monthly_series is None or monthly_series.empty:
            return

        top_months = monthly_series.nlargest(TOP_MONTHS_COUNT)

        for rank, (date, value) in enumerate(top_months.items(), 1):
            month_col = f'Top_{rank}_Month'
            value_col = f'Top_{rank}_Value'

            df.at[idx, month_col] = date.strftime('%Y-%m')
            df.at[idx, value_col] = round(float(value), 2)

        print(f"  📊 Top month: {top_months.index[0].strftime('%Y-%m')} (value: {top_months.iloc[0]:.2f})")

    def export_dashboard_json(self):
        """Export accumulated data as dashboard-ready JSON"""
        if not self.dashboard_data:
            print(f"\n⚠️  No dashboard data to export")
            return

        output = {
            "file_type": "Google Trends Data",
            "confidence": "high",
            "summary": f"Google Trends data for {len(set([d['product'] for d in self.dashboard_data]))} products across top {TOP_MONTHS_COUNT} demand months",
            "products_added": [],
            "demand_adjustments": self.dashboard_data,
            "alerts": [
                f"Data covers 5-year trends with focus on top {TOP_MONTHS_COUNT} months per product",
                "Demand scores converted from Google Trends (0-100) to dashboard scale (0-4)"
            ],
            "generated_at": datetime.now().isoformat(),
            "data_source": "Google Trends API (India)",
            "total_adjustments": len(self.dashboard_data)
        }

        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        print(f"\n✅ Dashboard JSON exported: {OUTPUT_JSON}")
        print(f"   📊 {len(self.dashboard_data)} demand adjustments")
        print(f"   📦 {len(set([d['product'] for d in self.dashboard_data]))} unique products")
        print(f"\n💡 Upload this JSON to your dashboard's Data Hub to apply trends!")

    def run_single_batch(self, df):
        """Run a single batch of 10 products"""
        remaining = self.get_remaining_indices(len(df))

        if not remaining:
            failed_count = len(self.progress['failed_attempts'])
            if failed_count == 0:
                return False, "ALL_COMPLETE"
            else:
                return False, "RETRYING_FAILED"

        if f'Top_1_Month' not in df.columns:
            self.initialize_top_months_columns(df)

        # Prioritize failed products
        failed_indices = [int(idx) for idx in self.progress['failed_attempts'].keys()]
        failed_to_retry = [idx for idx in failed_indices if idx in remaining]

        if failed_to_retry:
            num_failed = min(len(failed_to_retry), PRODUCTS_PER_BATCH)
            batch_indices = failed_to_retry[:num_failed]

            if num_failed < PRODUCTS_PER_BATCH:
                new_products = [idx for idx in remaining if idx not in failed_to_retry]
                random.shuffle(new_products)
                batch_indices.extend(new_products[:PRODUCTS_PER_BATCH - num_failed])

            batch_indices = sorted(batch_indices)
            print(f"📍 {num_failed} retry + {PRODUCTS_PER_BATCH - num_failed} new")
        else:
            random.shuffle(remaining)
            batch_indices = sorted(remaining[:PRODUCTS_PER_BATCH])
            print(f"🆕 {len(batch_indices)} new products")

        batch_num = self.progress['total_batches_completed'] + 1
        print(f"Batch #{batch_num}")

        start_time = time.time()
        success_count = 0

        for i, idx in enumerate(batch_indices):
            row = df.iloc[idx]
            search_term = row.get('Search Term', '')
            product = row.get('Product', 'Unknown')
            state = row.get('State', 'All India')
            category_group = row.get('Category Group', 'general')

            print(f"\n{'=' * 70}")
            print(f"[{i + 1}/{len(batch_indices)}] Row {idx + 1} - {product}")
            print(f"🔍 '{search_term}'")

            avg_interest, trend_direction, monthly_series = self.fetch_trends(search_term)

            # Store summary data in Excel
            df.at[idx, 'Avg_Interest_5Y'] = avg_interest
            df.at[idx, 'Trend_Direction'] = trend_direction
            df.at[idx, 'Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Store top months in Excel
            if monthly_series is not None:
                self.store_top_months(df, idx, monthly_series)

                # Convert to dashboard format and accumulate
                dashboard_adjustments = self.convert_to_dashboard_format(
                    product, state, category_group, monthly_series
                )
                self.dashboard_data.extend(dashboard_adjustments)
                print(f"  📊 Generated {len(dashboard_adjustments)} dashboard adjustments")

            if avg_interest is not None:
                success_count += 1
                data_points = len(monthly_series) if monthly_series is not None else 0
                print(f"  ✅ Interest: {avg_interest} | {trend_direction} | {data_points} months total")
                self.progress['scraped_indices'].append(idx)
                if str(idx) in self.progress['failed_attempts']:
                    del self.progress['failed_attempts'][str(idx)]
            else:
                print(f"  ❌ Failed")
                idx_str = str(idx)
                if idx_str not in self.progress['failed_attempts']:
                    self.progress['failed_attempts'][idx_str] = 0
                self.progress['failed_attempts'][idx_str] += 1

            self.save_progress()
            df.to_excel(OUTPUT_EXCEL, index=False, engine='openpyxl')

            # Export dashboard JSON after each successful product
            if avg_interest is not None:
                self.export_dashboard_json()

        self.progress['total_batches_completed'] += 1
        self.progress['last_batch_time'] = datetime.now().isoformat()
        self.save_progress()

        elapsed = time.time() - start_time
        print(f"\n{'=' * 70}")
        print(f"✅ BATCH #{batch_num} COMPLETE")
        print(f"⏱️  {elapsed / 60:.1f} minutes")
        print(f"✅ Success: {success_count}/{len(batch_indices)}")
        print(
            f"📊 Total progress: {len(self.progress['scraped_indices'])}/{len(df)} ({len(self.progress['scraped_indices']) / len(df) * 100:.1f}%)")
        print(f"{'=' * 70}")

        return True, None

    def run_continuous(self):
        """Run batches continuously with 18-23 minute waits"""
        print(f"""
╔═══════════════════════════════════════════════════════════════════╗
║   Google Trends → Dashboard Automation (Top {TOP_MONTHS_COUNT} Months)        ║
╚═══════════════════════════════════════════════════════════════════╝

📂 Input:  {INPUT_EXCEL}
📂 Output Excel: {OUTPUT_EXCEL}
📂 Output JSON: {OUTPUT_JSON} (Dashboard-ready!)
🔢 Batch size: {PRODUCTS_PER_BATCH} products
⏱️  Term delays: {MIN_SECONDS_BETWEEN_TERMS}-{MAX_SECONDS_BETWEEN_TERMS}s
⏱️  Batch delays: {MIN_MINUTES_BETWEEN_BATCHES}-{MAX_MINUTES_BETWEEN_BATCHES} minutes
📊 Period: {TIMEFRAME}
🌍 Geography: India
📅 Output: Top {TOP_MONTHS_COUNT} months → Dashboard JSON
""")

        # Load data
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
                df['Last_Updated'] = None
        except FileNotFoundError:
            print(f"❌ Could not find '{INPUT_EXCEL}'")
            return

        # Progress summary
        remaining = self.get_remaining_indices(len(df))
        scraped = len(self.progress['scraped_indices'])
        failed = len(self.progress['failed_attempts'])

        print(f"\n📊 Progress:")
        print(f"   Total: {len(df)}")
        print(f"   Scraped: {scraped}")
        if failed > 0:
            print(f"   Failed: {failed}")
        print(f"   Remaining: {len(remaining)}")
        print(f"   Batches completed: {self.progress['total_batches_completed']}")

        if not remaining and failed == 0:
            print(f"\n🎉 ALL DONE! {scraped}/{len(df)} products scraped!")
            self.export_dashboard_json()
            return

        print(f"\n🚀 Starting continuous scraping...\n")

        while True:
            has_more, status = self.run_single_batch(df)

            if not has_more:
                if status == "ALL_COMPLETE":
                    print(f"\n🎉🎉🎉 ALL PRODUCTS SCRAPED!")
                    print(f"Total batches: {self.progress['total_batches_completed']}")
                    print(f"Success rate: {len(self.progress['scraped_indices'])}/{len(df)}")
                    self.export_dashboard_json()
                    break
                elif status == "RETRYING_FAILED":
                    print(f"\n⚠️  No new products, but {len(self.progress['failed_attempts'])} failed")

            remaining_after = len(df) - len(self.progress['scraped_indices'])
            if remaining_after == 0 and len(self.progress['failed_attempts']) == 0:
                self.export_dashboard_json()
                break

            # Wait between batches
            wait_minutes = random.randint(MIN_MINUTES_BETWEEN_BATCHES, MAX_MINUTES_BETWEEN_BATCHES)
            wait_seconds = round(random.uniform(0, 59.99999), 5)
            total_wait_seconds = wait_minutes * 60 + wait_seconds

            next_time = datetime.now() + timedelta(seconds=total_wait_seconds)

            print(f"\n⏳ WAITING {wait_minutes} min {wait_seconds:.5f} sec until next batch")
            print(f"   Next batch at: {next_time.strftime('%I:%M:%S %p')}")
            print(f"   Remaining: {remaining_after} products")
            print(f"\n{'=' * 70}\n")

            time.sleep(total_wait_seconds)

        print(f"\n💾 Excel saved: {OUTPUT_EXCEL}")
        print(f"💾 Dashboard JSON saved: {OUTPUT_JSON}")
        print(f"\n📤 NEXT STEP: Upload '{OUTPUT_JSON}' to your dashboard's Data Hub!")


if __name__ == "__main__":
    try:
        scraper = DashboardTrendsScraper()
        scraper.run_continuous()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted! Progress saved. Run again to resume.")
        print(f"💾 Partial dashboard JSON available: {OUTPUT_JSON}")
    except Exception as e:
        print(f"\n\n❌ Error: {str(e)}")
        import traceback

        traceback.print_exc()
