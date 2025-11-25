#!/usr/bin/env python3
"""
Label permanently failed ad groups with THEME_XX_ATTEMPTED label.
This prevents them from appearing in future discoveries while allowing manual fixes later.

Usage: python label_failed_ad_groups.py --theme black_friday --job-ids 232,233,234
"""

import sys
import os
from pathlib import Path
from collections import defaultdict
import argparse

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))

from dotenv import load_dotenv
from config import load_config_from_env
from google_ads_client import initialize_client
from themes import get_theme_label

def get_failed_ad_groups(job_ids: list) -> dict:
    """Get permanently failed ad groups from database."""
    import psycopg2

    # Use db as hostname when running in Docker, localhost otherwise
    db_host = 'db' if os.path.exists('/.dockerenv') else 'localhost'

    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', db_host),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME', 'thema_ads'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres')
    )

    cur = conn.cursor()

    # Get failed items with permanent errors
    cur.execute("""
        SELECT DISTINCT customer_id, ad_group_id, error_message
        FROM thema_ads_job_items
        WHERE job_id = ANY(%s)
        AND status = 'failed'
        AND (
            error_message LIKE '%no resource returned%'
            OR error_message LIKE '%PROHIBITED_SYMBOLS%'
            OR error_message LIKE '%DESTINATION_NOT_WORKING%'
            OR error_message LIKE '%POLICY_FINDING%'
        )
        ORDER BY customer_id, ad_group_id
    """, (job_ids,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Group by customer
    by_customer = defaultdict(list)
    for customer_id, ad_group_id, error_message in rows:
        by_customer[customer_id].append({
            'ad_group_id': ad_group_id,
            'error_message': error_message
        })

    return by_customer


def ensure_label_exists(client, customer_id: str, label_name: str) -> str:
    """Ensure label exists, create if not, return resource name."""
    label_service = client.get_service("LabelService")
    ga_service = client.get_service("GoogleAdsService")

    # Check if label exists
    query = f"SELECT label.resource_name FROM label WHERE label.name = '{label_name}'"
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            print(f"  Label '{label_name}' already exists")
            return row.label.resource_name
    except Exception as e:
        pass

    # Create label
    print(f"  Creating label '{label_name}'")
    label_operation = client.get_type("LabelOperation")
    label = label_operation.create
    label.name = label_name

    try:
        response = label_service.mutate_labels(
            customer_id=customer_id,
            operations=[label_operation]
        )
        return response.results[0].resource_name
    except Exception as e:
        print(f"  Error creating label: {e}")
        raise


def label_ad_groups(client, customer_id: str, ad_group_ids: list, label_resource: str):
    """Apply label to ad groups."""
    ad_group_label_service = client.get_service("AdGroupLabelService")

    operations = []
    for ag_id in ad_group_ids:
        operation = client.get_type("AdGroupLabelOperation")
        ad_group_label = operation.create
        ad_group_label.ad_group = client.get_service("GoogleAdsService").ad_group_path(
            customer_id, ag_id
        )
        ad_group_label.label = label_resource
        operations.append(operation)

    # Batch in chunks of 5000
    BATCH_SIZE = 5000
    total_labeled = 0

    for i in range(0, len(operations), BATCH_SIZE):
        batch = operations[i:i + BATCH_SIZE]
        try:
            response = ad_group_label_service.mutate_ad_group_labels(
                customer_id=customer_id,
                operations=batch,
                partial_failure=False
            )
            total_labeled += len(response.results)
            print(f"  Labeled {len(response.results)} ad groups (batch {i//BATCH_SIZE + 1})")
        except Exception as e:
            print(f"  Error labeling batch: {e}")
            # Try individual operations to identify which ones fail
            for op in batch:
                try:
                    ad_group_label_service.mutate_ad_group_labels(
                        customer_id=customer_id,
                        operations=[op]
                    )
                    total_labeled += 1
                except Exception as e2:
                    print(f"    Failed to label ad group: {e2}")

    return total_labeled


def main():
    parser = argparse.ArgumentParser(description='Label permanently failed ad groups')
    parser.add_argument('--theme', required=True, help='Theme name (e.g., black_friday)')
    parser.add_argument('--job-ids', required=True, help='Comma-separated job IDs (e.g., 232,233,234)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')

    args = parser.parse_args()

    job_ids = [int(x.strip()) for x in args.job_ids.split(',')]
    theme = args.theme

    print(f"=== Labeling Permanently Failed Ad Groups ===")
    print(f"Theme: {theme}")
    print(f"Job IDs: {job_ids}")
    print(f"Dry run: {args.dry_run}")
    print()

    # Load config
    env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
    if not env_path.exists():
        print("ERROR: .env file not found")
        sys.exit(1)

    load_dotenv(env_path)
    config = load_config_from_env()
    client = initialize_client(config.google_ads)

    # Get theme label
    theme_label = get_theme_label(theme)
    attempted_label_name = f"{theme_label}_ATTEMPTED"

    print(f"Label to apply: {attempted_label_name}")
    print()

    # Get failed ad groups from database
    print("Fetching failed ad groups from database...")
    failed_by_customer = get_failed_ad_groups(job_ids)

    total_customers = len(failed_by_customer)
    total_ad_groups = sum(len(ags) for ags in failed_by_customer.values())

    print(f"Found {total_ad_groups} permanently failed ad groups across {total_customers} customers")
    print()

    if args.dry_run:
        print("DRY RUN - No changes will be made")
        for customer_id, ad_groups in failed_by_customer.items():
            print(f"Customer {customer_id}: {len(ad_groups)} ad groups would be labeled")
        return

    # Process each customer
    total_labeled = 0
    for idx, (customer_id, ad_groups) in enumerate(failed_by_customer.items(), 1):
        print(f"[{idx}/{total_customers}] Processing customer {customer_id} ({len(ad_groups)} ad groups)")

        try:
            # Ensure label exists
            label_resource = ensure_label_exists(client, customer_id, attempted_label_name)

            # Label ad groups
            ad_group_ids = [ag['ad_group_id'] for ag in ad_groups]
            labeled = label_ad_groups(client, customer_id, ad_group_ids, label_resource)
            total_labeled += labeled

            print(f"  ✓ Successfully labeled {labeled} ad groups")
        except Exception as e:
            print(f"  ✗ Error processing customer: {e}")

        print()

    print(f"=== Complete ===")
    print(f"Total ad groups labeled: {total_labeled} / {total_ad_groups}")


if __name__ == "__main__":
    main()
