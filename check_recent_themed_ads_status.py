"""
Check recently created themed ads to see if any are ENABLED instead of PAUSED.
"""
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import os
from datetime import datetime, timedelta

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Add paths
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))

from config import load_config_from_env
from google_ads_client import initialize_client

config = load_config_from_env()
client = initialize_client(config.google_ads)
ga_service = client.get_service('GoogleAdsService')

# Get all customers
conn = psycopg2.connect(
    os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/thema_ads")
)
cur = conn.cursor()
cur.execute("SELECT DISTINCT customer_id FROM thema_ads_job_items WHERE customer_id IS NOT NULL ORDER BY customer_id")
customer_ids = [str(row[0]) for row in cur.fetchall()]
conn.close()

print(f"Checking {len(customer_ids)} customers for recently created themed ads...")
print()

# Check for ads created in the last 2 hours with themed URLs
enabled_count = 0
paused_count = 0
total_checked = 0

# Only check first few customers where we saw ads being created
customers_to_check = customer_ids[:5]  # First 5 customers for speed

for customer_id in customers_to_check:
    try:
        # Find ads with themed URLs (path1 parameter) created recently
        query = """
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.status,
                ad_group_ad.ad.final_urls
            FROM ad_group_ad
            WHERE ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.status IN (ENABLED, PAUSED)
        """

        response = ga_service.search(customer_id=customer_id, query=query)

        customer_enabled = 0
        customer_paused = 0

        for row in response:
            # Check if this ad has a themed URL (path1 with theme keywords)
            has_themed_url = False
            for url in row.ad_group_ad.ad.final_urls:
                if any(theme in url.lower() for theme in ['black_friday', 'cyber_monday', 'sinterklaas', 'kerstmis']):
                    has_themed_url = True
                    break

            if has_themed_url:
                total_checked += 1
                if row.ad_group_ad.status.name == 'ENABLED':
                    customer_enabled += 1
                    enabled_count += 1
                    # Show first few examples
                    if enabled_count <= 10:
                        print(f"  ⚠️  ENABLED themed ad found:")
                        print(f"      Customer: {customer_id}")
                        print(f"      Ad Group: {row.ad_group.name[:60]}")
                        print(f"      Ad ID: {row.ad_group_ad.ad.id}")
                        print(f"      URL: {row.ad_group_ad.ad.final_urls[0][:100]}")
                        print()
                elif row.ad_group_ad.status.name == 'PAUSED':
                    customer_paused += 1
                    paused_count += 1

        if customer_enabled > 0 or customer_paused > 0:
            print(f"Customer {customer_id}: {customer_enabled} ENABLED, {customer_paused} PAUSED")

    except Exception as e:
        print(f"Error checking customer {customer_id}: {e}")
        continue

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Total themed ads checked: {total_checked}")
print(f"ENABLED themed ads: {enabled_count}")
print(f"PAUSED themed ads: {paused_count}")
print()
if enabled_count > 0:
    print(f"⚠️  WARNING: {enabled_count} themed ads were created with ENABLED status")
    print("These should be paused to avoid the 3-RSA limit issues.")
else:
    print("✓ All themed ads have correct PAUSED status")
print("=" * 80)
