#!/usr/bin/env python3
"""Retrieve ad group names from Google Ads API"""
import sys
from pathlib import Path

# Add thema_ads_optimized to path
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))

from dotenv import load_dotenv
from config import load_config_from_env
from google_ads_client import initialize_client

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

config = load_config_from_env()
client = initialize_client(config.google_ads)
ga_service = client.get_service("GoogleAdsService")

customer_id = "4056770576"
ad_group_ids = [
    "167574689741",
    "167574690701",
    "167574692541",
    "167574693101",
    "167574695501",
    "167574696141",
    "167574696421",
    "167574696701",
    "167574697861",
    "167574699101"
]

print(f"Fetching ad group names for customer {customer_id}...\n")

for ad_group_id in ad_group_ids:
    query = f"""
        SELECT
            ad_group.id,
            ad_group.name,
            campaign.name
        FROM ad_group
        WHERE ad_group.id = {ad_group_id}
        LIMIT 1
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            print(f"Campaign: {row.campaign.name}")
            print(f"Ad Group ID: {row.ad_group.id}")
            print(f"Ad Group Name: {row.ad_group.name}")
            print("-" * 50)
    except Exception as e:
        print(f"Error fetching ad group {ad_group_id}: {e}")
        print("-" * 50)
