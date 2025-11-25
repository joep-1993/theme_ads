"""
Investigate specific ad group to see what themed ads it has.
"""
import sys
from pathlib import Path
from dotenv import load_dotenv

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

# Campaign ID provided by user
campaign_id = '20428646061'
ad_group_pattern = 'kantoorartikelen_558040_14926322'

# Get all customers
import psycopg2
import os
conn = psycopg2.connect(
    os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/thema_ads")
)
cur = conn.cursor()
cur.execute("SELECT DISTINCT customer_id FROM thema_ads_job_items WHERE customer_id IS NOT NULL")
customer_ids = [str(row[0]) for row in cur.fetchall()]
conn.close()

print(f"Searching for campaign {campaign_id} across {len(customer_ids)} customers...")

# Find the ad group
found_customer = None
found_ad_group = None

for customer_id in customer_ids:
    try:
        query = f'''
            SELECT
                ad_group.id,
                ad_group.name,
                campaign.name
            FROM ad_group
            WHERE campaign.id = {campaign_id}
                AND ad_group.name LIKE '%{ad_group_pattern}%'
            LIMIT 1
        '''
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            found_customer = customer_id
            found_ad_group = row.ad_group.id
            print(f"\n✓ Found ad group in customer: {customer_id}")
            print(f"  Ad Group ID: {row.ad_group.id}")
            print(f"  Ad Group Name: {row.ad_group.name}")
            print(f"  Campaign Name: {row.campaign.name}")
            break
    except Exception:
        continue

    if found_customer:
        break

if not found_customer:
    print("ERROR: Ad group not found in any customer!")
    sys.exit(1)

# Now fetch all RSAs in this ad group
print(f"\nFetching all RSAs in ad group {found_ad_group}...")

query = f'''
    SELECT
        ad_group_ad.ad.id,
        ad_group_ad.ad.responsive_search_ad.headlines,
        ad_group_ad.ad.responsive_search_ad.descriptions,
        ad_group_ad.ad.final_urls,
        ad_group_ad.status
    FROM ad_group_ad
    WHERE ad_group_ad.ad_group = 'customers/{found_customer}/adGroups/{found_ad_group}'
        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
        AND ad_group_ad.status != REMOVED
'''

response = ga_service.search(customer_id=found_customer, query=query)

ads = []
for row in response:
    ad_id = row.ad_group_ad.ad.id
    final_urls = [url for url in row.ad_group_ad.ad.final_urls]
    url_path1 = None
    if final_urls:
        # Extract path1 from URL parameters
        import urllib.parse
        parsed = urllib.parse.urlparse(final_urls[0])
        params = urllib.parse.parse_qs(parsed.query)
        url_path1 = params.get('path1', [None])[0]

    ads.append({
        'id': ad_id,
        'path1': url_path1,
        'status': row.ad_group_ad.status.name
    })

print(f"\nFound {len(ads)} RSAs in this ad group:")
for ad in ads:
    print(f"  - Ad ID: {ad['id']}, Path1: {ad['path1']}, Status: {ad['status']}")

# Now check labels on these ads
print(f"\nChecking labels on ads...")

if ads:
    ad_ids = [str(ad['id']) for ad in ads]
    ad_ids_str = ','.join(ad_ids)

    query = f'''
        SELECT
            ad_group_ad.ad.id,
            label.name
        FROM ad_group_ad_label
        WHERE ad_group_ad.ad_group = 'customers/{found_customer}/adGroups/{found_ad_group}'
            AND ad_group_ad.ad.id IN ({ad_ids_str})
    '''

    try:
        response = ga_service.search(customer_id=found_customer, query=query)
        ad_labels = {}
        for row in response:
            ad_id = row.ad_group_ad.ad.id
            label_name = row.label.name
            if ad_id not in ad_labels:
                ad_labels[ad_id] = []
            ad_labels[ad_id].append(label_name)

        print(f"\nLabels by ad:")
        for ad in ads:
            labels = ad_labels.get(ad['id'], [])
            theme_labels = [l for l in labels if l.startswith('THEME_')]
            print(f"  Ad {ad['id']} (path1={ad['path1']}): {', '.join(theme_labels) if theme_labels else 'NO THEME LABELS'}")
    except Exception as e:
        print(f"Error fetching labels: {e}")

# Summary
print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}")
print(f"Customer ID: {found_customer}")
print(f"Campaign ID: {campaign_id}")
print(f"Ad Group ID: {found_ad_group}")
print(f"Total RSAs: {len(ads)}")
themed_ads = [ad for ad in ads if ad['path1']]
print(f"Themed ads (with path1): {len(themed_ads)}")
print(f"\nThemes found:")
themes_found = set()
for ad in themed_ads:
    if ad['path1']:
        themes_found.add(ad['path1'].lower())
for theme in sorted(themes_found):
    print(f"  - {theme}")
print(f"{'='*80}")
