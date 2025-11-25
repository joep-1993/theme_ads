#!/usr/bin/env python3
"""Quick verification of theme ad coverage for random ad groups."""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Sample ad groups to check (expanded sample from different customers)
SAMPLES = [
    ('2237802672', '158979861290', 'Wasdrogers - black friday missing'),
    ('3273661472', '180089339468', 'Spierbalsems - black friday missing'),
    ('4675585929', '174460250075', 'Toilettassen - black friday missing'),
    # Additional samples from different customers
    ('1351439239', '143263956719', 'Customer 1351439239 - ad group 1'),
    ('1351439239', '139176998850', 'Customer 1351439239 - ad group 2'),
    ('9828462127', '176192083121', 'Customer 9828462127'),
    ('2569129112', '145401645513', 'Customer 2569129112'),
    ('9099508603', '151015412080', 'Customer 9099508603'),
]

def check_ad_group(client, customer_id, ad_group_id, description):
    """Check if ad group has all 4 theme ads."""
    ga_service = client.get_service('GoogleAdsService')

    # First get ad group and campaign names
    ad_group_query = f"""
        SELECT
            ad_group.id,
            ad_group.name,
            campaign.name
        FROM ad_group
        WHERE ad_group.id = {ad_group_id}
    """

    ad_group_name = "Unknown"
    campaign_name = "Unknown"
    try:
        ag_response = ga_service.search(customer_id=customer_id, query=ad_group_query)
        for row in ag_response:
            ad_group_name = row.ad_group.name
            campaign_name = row.campaign.name
            break
    except Exception as e:
        pass

    # Query for theme ads
    query = f"""
        SELECT
            ad_group_ad.ad.id,
            ad_group_ad.ad.responsive_search_ad.path1,
            ad_group_ad.status
        FROM ad_group_ad
        WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
        AND ad_group_ad.ad.responsive_search_ad.path1 IN ('black_friday', 'cyber_monday', 'sinterklaas', 'kerstmis')
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=query)

        themes_found = set()
        for row in response:
            path1 = row.ad_group_ad.ad.responsive_search_ad.path1
            themes_found.add(path1)

        print(f"\n{'='*80}")
        print(f"Ad Group: {ad_group_name}")
        print(f"Ad Group ID: {ad_group_id}")
        print(f"Campaign: {campaign_name}")
        print(f"Customer: {customer_id}")
        print(f"{'='*80}")
        print(f"Themes found: {len(themes_found)}/4\n")

        for theme in ['black_friday', 'cyber_monday', 'sinterklaas', 'kerstmis']:
            if theme in themes_found:
                print(f"  ✓ {theme:20}")
            else:
                print(f"  ✗ {theme:20} MISSING")

        if len(themes_found) == 4:
            print("\n✅ All 4 themes present!")
        else:
            print(f"\n⚠️  Only {len(themes_found)}/4 themes present")

        return len(themes_found) == 4

    except Exception as e:
        print(f"\n❌ Error checking ad group {ad_group_id}: {e}")
        return False


def main():
    # Initialize Google Ads client
    config = {
        'developer_token': os.environ.get('GOOGLE_DEVELOPER_TOKEN'),
        'client_id': os.environ.get('GOOGLE_CLIENT_ID'),
        'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET'),
        'refresh_token': os.environ.get('GOOGLE_REFRESH_TOKEN'),
        'login_customer_id': os.environ.get('GOOGLE_LOGIN_CUSTOMER_ID'),
        'use_proto_plus': True
    }
    client = GoogleAdsClient.load_from_dict(config)
    print("Google Ads client initialized successfully\n")

    complete_count = 0
    incomplete_count = 0

    for customer_id, ad_group_id, description in SAMPLES:
        is_complete = check_ad_group(client, customer_id, ad_group_id, description)
        if is_complete:
            complete_count += 1
        else:
            incomplete_count += 1

    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total checked: {len(SAMPLES)}")
    print(f"Complete (4/4 themes): {complete_count}")
    print(f"Incomplete: {incomplete_count}")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    main()
