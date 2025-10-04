#!/usr/bin/env python3
"""
Get names of customer accounts that have permission errors
"""
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

# Customer IDs with errors
error_customer_ids = [
    "1156706227",
    "1315864723",
    "1444916021",
    "2711091729",
    "2843364513",
    "3950176939",
    "5036996725",
    "5247789523",
    "5342845725",
    "6164655666",
    "7002469725",
    "7530405184",
    "8027913631",
    "8532448088",
    "8802169982",
    "9256489127",
    "9739797031"
]

manager_account_id = "1103539935"
ga_service = client.get_service("GoogleAdsService")

# Query for customer names
customer_query = """
    SELECT
        customer_client.descriptive_name,
        customer_client.id,
        customer_client.status
    FROM customer_client
    WHERE customer_client.manager = FALSE
"""

response = ga_service.search(customer_id=manager_account_id, query=customer_query)

print("\nCustomer accounts with permission errors:\n")
print(f"{'Customer ID':<15} {'Status':<20} {'Name'}")
print("-" * 80)

error_accounts_found = []
for row in response:
    customer_id = str(row.customer_client.id)
    if customer_id in error_customer_ids:
        name = row.customer_client.descriptive_name
        status = row.customer_client.status.name
        error_accounts_found.append({
            'id': customer_id,
            'name': name,
            'status': status
        })
        print(f"{customer_id:<15} {status:<20} {name}")

print(f"\nTotal: {len(error_accounts_found)} accounts with errors")
