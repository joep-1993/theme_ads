import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Initialize Google Ads client
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))
from config import load_config_from_env
from google_ads_client import initialize_client

config = load_config_from_env()
client = initialize_client(config.google_ads)
ga_service = client.get_service("GoogleAdsService")

# Check ad groups
customer_id = "4056770576"
ad_group_ids = ["163249683057", "163499699025", "163541062983"]

for ag_id in ad_group_ids:
    print(f"\n{'='*80}")
    print(f"Ad Group ID: {ag_id}")

    # Check ad group labels
    ag_label_query = f"""
        SELECT ad_group.id, ad_group.name, ad_group_label.label
        FROM ad_group_label
        WHERE ad_group.id = {ag_id}
    """

    labels = []
    try:
        ag_response = ga_service.search(customer_id=customer_id, query=ag_label_query)
        for row in ag_response:
            label_resource = row.ad_group_label.label
            # Get label name
            label_name_query = f"SELECT label.name FROM label WHERE label.resource_name = '{label_resource}'"
            label_response = ga_service.search(customer_id=customer_id, query=label_name_query)
            for label_row in label_response:
                labels.append(label_row.label.name)
                break
    except Exception as e:
        print(f"  Error fetching labels: {e}")

    print(f"  Ad Group Labels: {labels}")
    has_km_done = "THEME_KM_DONE" in labels
    print(f"  Has THEME_KM_DONE: {has_km_done}")

    # Check ads in ad group
    ads_query = f"""
        SELECT
            ad_group_ad.ad.id,
            ad_group_ad.ad.responsive_search_ad.headlines
        FROM ad_group_ad
        WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ag_id}'
        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
        AND ad_group_ad.status != REMOVED
    """

    try:
        ads_response = ga_service.search(customer_id=customer_id, query=ads_query)
        ad_count = 0
        has_km_ad = False
        for row in ads_response:
            ad_count += 1
            ad_id = row.ad_group_ad.ad.id
            headlines = [h.text for h in row.ad_group_ad.ad.responsive_search_ad.headlines[:3]]  # First 3

            # Check if this ad has theme labels
            ad_label_query = f"""
                SELECT ad_group_ad_label.label
                FROM ad_group_ad_label
                WHERE ad_group_ad_label.ad_group_ad = 'customers/{customer_id}/adGroups/{ag_id}/ads/{ad_id}'
            """

            ad_labels = []
            try:
                ad_label_response = ga_service.search(customer_id=customer_id, query=ad_label_query)
                for label_row in ad_label_response:
                    label_resource = label_row.ad_group_ad_label.label
                    # Get label name
                    label_name_query = f"SELECT label.name FROM label WHERE label.resource_name = '{label_resource}'"
                    label_response = ga_service.search(customer_id=customer_id, query=label_name_query)
                    for ln_row in label_response:
                        ad_labels.append(ln_row.label.name)
                        break
            except Exception as e:
                pass

            if "THEME_KM" in ad_labels:
                has_km_ad = True

            theme_labels = [l for l in ad_labels if l.startswith("THEME_")]
            print(f"  Ad {ad_id}: Headlines: {', '.join(headlines[:50])}... Theme Labels: {theme_labels}")

        print(f"  Total RSAs: {ad_count}")
        print(f"  Has THEME_KM ad: {has_km_ad}")
    except Exception as e:
        print(f"  Error fetching ads: {e}")
