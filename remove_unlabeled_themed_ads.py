#!/usr/bin/env python3
"""
Remove ads that have theme path1 values but no corresponding theme labels.
This catches ads created by broken scripts that didn't add labels properly.
"""

import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from concurrent.futures import ProcessPoolExecutor, as_completed
import argparse

# Load environment variables
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Theme mappings
THEME_PATH_TO_LABEL = {
    'black_friday': 'THEME_BF',
    'cyber_monday': 'THEME_CM',
    'sinterklaas': 'THEME_SK',
    'kerstmis': 'THEME_KM'
}


def get_customer_ids():
    """Load customer IDs from whitelist file."""
    try:
        with open('thema_ads_optimized/account ids', 'r') as f:
            customer_ids = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(customer_ids)} customer IDs from whitelist")
        return customer_ids
    except Exception as e:
        logger.error(f"Error loading customer IDs: {e}")
        return []


def find_unlabeled_themed_ads(client, customer_id: str, dry_run: bool = True):
    """Find ads with theme path1 but no corresponding theme label."""
    ga_service = client.get_service('GoogleAdsService')

    logger.info(f"Querying ads with theme path1 values for customer {customer_id}...")

    # Query each theme separately (GAQL doesn't support OR in WHERE clause)
    ads_with_path1 = []

    try:
        for path1_value in THEME_PATH_TO_LABEL.keys():
            query = f"""
                SELECT
                    ad_group_ad.resource_name,
                    ad_group_ad.ad.id,
                    ad_group_ad.ad.responsive_search_ad.path1,
                    ad_group_ad.status,
                    ad_group.id,
                    ad_group.name,
                    campaign.id,
                    campaign.name
                FROM ad_group_ad
                WHERE ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.ad.responsive_search_ad.path1 = '{path1_value}'
            """

            response = ga_service.search(customer_id=customer_id, query=query)

            for row in response:
                ad_resource = row.ad_group_ad.resource_name
                ad_id = row.ad_group_ad.ad.id
                path1 = row.ad_group_ad.ad.responsive_search_ad.path1
                status = row.ad_group_ad.status.name
                ad_group_name = row.ad_group.name

                ads_with_path1.append({
                    'resource_name': ad_resource,
                    'ad_id': ad_id,
                    'path1': path1,
                    'status': status,
                    'ad_group_name': ad_group_name,
                    'expected_label': THEME_PATH_TO_LABEL.get(path1)
                })

        logger.info(f"Found {len(ads_with_path1)} ads with theme path1 values")

        if not ads_with_path1:
            return []

        # Now check which of these ads have the corresponding label
        ad_resources = [ad['resource_name'] for ad in ads_with_path1]

        # Batch check labels in chunks of 5000
        BATCH_SIZE = 5000
        ad_labels_map = {}  # resource_name -> [label_names]

        for i in range(0, len(ad_resources), BATCH_SIZE):
            batch = ad_resources[i:i + BATCH_SIZE]
            batch_str = ", ".join([f"'{r}'" for r in batch])

            label_query = f"""
                SELECT
                    ad_group_ad_label.ad_group_ad,
                    ad_group_ad_label.label
                FROM ad_group_ad_label
                WHERE ad_group_ad_label.ad_group_ad IN ({batch_str})
            """

            label_response = ga_service.search(customer_id=customer_id, query=label_query)

            # Collect label resources
            label_resources = set()
            for row in label_response:
                ad_resource = row.ad_group_ad_label.ad_group_ad
                label_resource = row.ad_group_ad_label.label

                if ad_resource not in ad_labels_map:
                    ad_labels_map[ad_resource] = []
                ad_labels_map[ad_resource].append(label_resource)
                label_resources.add(label_resource)

            # Fetch label names
            if label_resources:
                label_resources_str = ", ".join([f"'{r}'" for r in label_resources])
                label_name_query = f"""
                    SELECT
                        label.resource_name,
                        label.name
                    FROM label
                    WHERE label.resource_name IN ({label_resources_str})
                """

                label_name_response = ga_service.search(customer_id=customer_id, query=label_name_query)
                label_name_map = {row.label.resource_name: row.label.name for row in label_name_response}

                # Map ad resources to label names
                for ad_resource, label_resources_list in ad_labels_map.items():
                    ad_labels_map[ad_resource] = [label_name_map.get(lr) for lr in label_resources_list]

        # Filter ads that don't have their expected label
        ads_to_remove = []
        for ad in ads_with_path1:
            ad_resource = ad['resource_name']
            expected_label = ad['expected_label']
            actual_labels = ad_labels_map.get(ad_resource, [])

            if expected_label not in actual_labels:
                ads_to_remove.append(ad)
                logger.info(f"  Ad {ad['ad_id']} has path1='{ad['path1']}' but missing label '{expected_label}'")
                logger.info(f"    Ad Group: {ad['ad_group_name']}")
                logger.info(f"    Status: {ad['status']}")
                logger.info(f"    Actual labels: {actual_labels}")

        logger.info(f"Found {len(ads_to_remove)} ads to remove (have path1 but missing label)")
        return ads_to_remove

    except GoogleAdsException as ex:
        logger.error(f"Error querying ads for customer {customer_id}: {ex}")
        return []


def remove_ads_batch(client, customer_id: str, ads_to_remove: list, dry_run: bool = True):
    """Remove ads in batch."""
    if not ads_to_remove:
        return 0

    if dry_run:
        logger.info(f"[DRY RUN] Would remove {len(ads_to_remove)} ads")
        return 0

    ad_group_ad_service = client.get_service('AdGroupAdService')

    # Batch removal in chunks of 100 (small batches to avoid API errors)
    BATCH_SIZE = 100
    total_removed = 0

    for i in range(0, len(ads_to_remove), BATCH_SIZE):
        batch = ads_to_remove[i:i + BATCH_SIZE]
        operations = []

        for ad in batch:
            operation = client.get_type('AdGroupAdOperation')
            operation.remove = ad['resource_name']
            operations.append(operation)

        try:
            response = ad_group_ad_service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=operations
            )
            total_removed += len(response.results)
            logger.info(f"  Removed batch of {len(response.results)} ads (total: {total_removed}/{len(ads_to_remove)})")
        except GoogleAdsException as ex:
            logger.error(f"Error removing ads batch: {ex}")

    return total_removed


def process_customer(customer_id: str, dry_run: bool = True):
    """Process a single customer."""
    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing customer {customer_id}")
        logger.info(f"{'='*80}")

        # Initialize Google Ads client from environment variables
        config = {
            'developer_token': os.environ.get('GOOGLE_DEVELOPER_TOKEN'),
            'client_id': os.environ.get('GOOGLE_CLIENT_ID'),
            'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET'),
            'refresh_token': os.environ.get('GOOGLE_REFRESH_TOKEN'),
            'login_customer_id': os.environ.get('GOOGLE_LOGIN_CUSTOMER_ID'),
            'use_proto_plus': True
        }
        client = GoogleAdsClient.load_from_dict(config)
        logger.info("Google Ads client initialized successfully")

        # Find unlabeled themed ads
        ads_to_remove = find_unlabeled_themed_ads(client, customer_id, dry_run)

        if not ads_to_remove:
            logger.info(f"Customer {customer_id}: No unlabeled themed ads found")
            return (customer_id, 0, 0)

        # Remove ads
        removed_count = remove_ads_batch(client, customer_id, ads_to_remove, dry_run)

        logger.info(f"\nCustomer {customer_id} Summary: Found {len(ads_to_remove)} unlabeled themed ads, removed {removed_count}")

        return (customer_id, len(ads_to_remove), removed_count)

    except Exception as e:
        logger.error(f"Error processing customer {customer_id}: {e}")
        return (customer_id, 0, 0)


def main():
    parser = argparse.ArgumentParser(description='Remove ads with theme path1 but no labels')
    parser.add_argument('--execute', action='store_true', help='Execute removal (default is dry-run)')
    parser.add_argument('--parallel', type=int, default=3, help='Number of parallel workers (default: 3)')
    parser.add_argument('--customer-limit', type=int, help='Limit to first N customers (for testing)')
    args = parser.parse_args()

    dry_run = not args.execute

    logger.info("="*80)
    logger.info(f"{'DRY RUN' if dry_run else 'EXECUTE'} MODE (PARALLEL: {args.parallel} workers)")
    logger.info("="*80)

    if not dry_run:
        confirm = input("Are you sure you want to REMOVE unlabeled themed ads? (yes/no): ")
        if confirm.lower() != 'yes':
            logger.info("Aborted by user")
            return

    # Get customer IDs
    customer_ids = get_customer_ids()
    if not customer_ids:
        logger.error("No customer IDs found")
        return

    if args.customer_limit:
        customer_ids = customer_ids[:args.customer_limit]
        logger.info(f"Limited to first {args.customer_limit} customers")

    logger.info(f"Processing {len(customer_ids)} customers with {args.parallel} parallel workers")

    # Process in parallel
    results = []
    with ProcessPoolExecutor(max_workers=args.parallel) as executor:
        futures = {executor.submit(process_customer, cid, dry_run): cid for cid in customer_ids}

        for future in as_completed(futures):
            customer_id, found_count, removed_count = future.result()
            results.append((customer_id, found_count, removed_count))
            logger.info(f"✓ Completed customer {customer_id} - Found: {found_count}, Removed: {removed_count}")

    # Summary
    logger.info("\n" + "="*80)
    logger.info("GRAND TOTAL")
    logger.info("="*80)

    total_found = sum(r[1] for r in results)
    total_removed = sum(r[2] for r in results)

    logger.info(f"Total unlabeled themed ads found: {total_found}")
    logger.info(f"Total ads removed: {total_removed}")
    logger.info(f"Customers processed: {len(results)}")


if __name__ == '__main__':
    main()
