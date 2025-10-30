#!/usr/bin/env python3
"""
Audit BF_DONE labels - Remove label if no actual Black Friday ad exists.

This script:
1. Finds all ad groups with THEME_BF_DONE label
2. Checks if they actually have a Black Friday ad (path1='black_friday')
3. Removes THEME_BF_DONE label if no BF ad exists
4. Allows auto-discover to pick them up later
"""

import os
import sys
import logging
import argparse
import psycopg2
from pathlib import Path
from typing import List, Tuple, Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.api_core import protobuf_helpers

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_google_ads_client():
    """Initialize Google Ads client."""
    config = {
        'developer_token': os.environ.get('GOOGLE_DEVELOPER_TOKEN'),
        'client_id': os.environ.get('GOOGLE_CLIENT_ID'),
        'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET'),
        'refresh_token': os.environ.get('GOOGLE_REFRESH_TOKEN'),
        'login_customer_id': os.environ.get('GOOGLE_LOGIN_CUSTOMER_ID'),
        'use_proto_plus': True
    }
    return GoogleAdsClient.load_from_dict(config)


def get_all_customers() -> List[str]:
    """Get all customer IDs from the database (customers that have been processed before)."""
    conn = psycopg2.connect(
        os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/thema_ads")
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT customer_id
        FROM thema_ads_job_items
        WHERE customer_id IS NOT NULL
        ORDER BY customer_id
    """)
    customers = [str(row[0]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    logger.info(f"Found {len(customers)} customer accounts in database")
    return customers


def audit_customer(customer_id: str, theme_code: str, theme_path: str, dry_run: bool = True) -> Dict:
    """
    Audit one customer:
    1. Find ad groups with THEME_XX_DONE label
    2. Check if they have actual theme ads
    3. Remove label if no theme ad exists
    """
    client = get_google_ads_client()
    ga_service = client.get_service('GoogleAdsService')
    label_service = client.get_service('LabelService')
    ad_group_label_service = client.get_service('AdGroupLabelService')

    stats = {
        'customer_id': customer_id,
        'ad_groups_with_done_label': 0,
        'ad_groups_missing_theme_ad': 0,
        'labels_removed': 0,
        'errors': 0
    }

    logger.info(f"\n{'='*80}")
    logger.info(f"Customer: {customer_id}")
    logger.info(f"{'='*80}")

    done_label_name = f"THEME_{theme_code}_DONE"

    try:
        # Step 1: Get THEME_XX_DONE label ID
        label_query = f"""
            SELECT label.id, label.name
            FROM label
            WHERE label.name = '{done_label_name}'
        """

        label_response = ga_service.search(customer_id=customer_id, query=label_query)
        done_label_id = None
        for row in label_response:
            done_label_id = row.label.id
            break

        if not done_label_id:
            logger.info(f"  No {done_label_name} label found - skipping customer")
            return stats

        logger.info(f"  Found {done_label_name} label: {done_label_id}")

        # Step 2: Get all ad groups with THEME_XX_DONE label
        ad_group_query = f"""
            SELECT
                ad_group.id,
                ad_group.name,
                campaign.name,
                ad_group.status,
                campaign.status
            FROM ad_group_label
            WHERE ad_group_label.label = 'customers/{customer_id}/labels/{bf_done_label_id}'
            AND ad_group.status = 'ENABLED'
            AND campaign.status = 'ENABLED'
        """

        ag_response = ga_service.search(customer_id=customer_id, query=ad_group_query)
        ad_groups = []
        for row in ag_response:
            ad_groups.append({
                'id': row.ad_group.id,
                'name': row.ad_group.name,
                'campaign_name': row.campaign.name
            })

        stats['ad_groups_with_done_label'] = len(ad_groups)
        logger.info(f"  Found {len(ad_groups)} ad groups with THEME_BF_DONE label")

        if not ad_groups:
            return stats

        # Step 3: Check each ad group for actual BF ads
        for ag in ad_groups:
            ag_id = ag['id']

            # Query for Black Friday ads
            bf_ad_query = f"""
                SELECT
                    ad_group_ad.ad.id
                FROM ad_group_ad
                WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ag_id}'
                AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.ad.responsive_search_ad.path1 = 'black_friday'
            """

            try:
                bf_ad_response = ga_service.search(customer_id=customer_id, query=bf_ad_query)
                has_bf_ad = False
                for _ in bf_ad_response:
                    has_bf_ad = True
                    break

                if not has_bf_ad:
                    # No BF ad found - remove the DONE label
                    stats['ad_groups_missing_bf_ad'] += 1
                    logger.info(f"    ⚠️  Ad Group {ag_id} ({ag['name'][:50]}) - MISSING BF ad")

                    if not dry_run:
                        # Remove the THEME_BF_DONE label
                        ad_group_label_resource = ad_group_label_service.ad_group_label_path(
                            customer_id, ag_id, bf_done_label_id
                        )

                        operation = client.get_type('AdGroupLabelOperation')
                        operation.remove = ad_group_label_resource

                        ad_group_label_service.mutate_ad_group_labels(
                            customer_id=customer_id,
                            operations=[operation]
                        )
                        stats['labels_removed'] += 1
                        logger.info(f"      ✓ Removed THEME_BF_DONE label")
                    else:
                        logger.info(f"      [DRY RUN] Would remove THEME_BF_DONE label")

            except Exception as e:
                logger.error(f"    Error checking ad group {ag_id}: {e}")
                stats['errors'] += 1

        logger.info(f"\n  Customer {customer_id} Summary:")
        logger.info(f"    Ad groups with THEME_BF_DONE: {stats['ad_groups_with_done_label']}")
        logger.info(f"    Missing BF ads: {stats['ad_groups_missing_bf_ad']}")
        if not dry_run:
            logger.info(f"    Labels removed: {stats['labels_removed']}")
        logger.info(f"    Errors: {stats['errors']}")

    except Exception as e:
        logger.error(f"  Error processing customer {customer_id}: {e}")
        stats['errors'] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description='Audit theme DONE labels and remove if no actual theme ad exists')
    parser.add_argument('--execute', action='store_true', help='Actually remove labels (default is dry run)')
    parser.add_argument('--parallel', type=int, default=3, help='Number of parallel workers (default: 3)')
    parser.add_argument('--customer-id', type=str, help='Process only specific customer ID')
    parser.add_argument('--theme', type=str, default='BF', choices=['BF', 'CM', 'SK', 'KM'],
                       help='Theme to audit: BF=black_friday, CM=cyber_monday, SK=sinterklaas, KM=kerstmis')
    args = parser.parse_args()

    dry_run = not args.execute

    logger.info("="*80)
    if dry_run:
        logger.info("DRY RUN MODE - No labels will be removed")
    else:
        logger.info(f"EXECUTE MODE (PARALLEL: {args.parallel} workers)")
    logger.info("="*80)

    if not dry_run:
        confirm = input("Are you sure you want to REMOVE incorrect BF_DONE labels? (yes/no): ")
        if confirm.lower() != 'yes':
            logger.info("Aborted by user")
            return

    # Get customers
    if args.customer_id:
        customers = [args.customer_id]
    else:
        customers = get_all_customers()

    if not customers:
        logger.error("No customers found")
        return

    logger.info(f"Processing {len(customers)} customers with {args.parallel} parallel workers")

    # Process customers in parallel
    all_stats = []

    if args.parallel > 1:
        with ProcessPoolExecutor(max_workers=args.parallel) as executor:
            future_to_customer = {
                executor.submit(audit_customer, customer_id, dry_run): customer_id
                for customer_id in customers
            }

            for future in as_completed(future_to_customer):
                customer_id = future_to_customer[future]
                try:
                    stats = future.result()
                    all_stats.append(stats)
                    logger.info(f"✓ Completed customer {customer_id} - Missing BF: {stats['ad_groups_missing_bf_ad']}, Labels removed: {stats['labels_removed']}")
                except Exception as e:
                    logger.error(f"✗ Failed customer {customer_id}: {e}")
    else:
        for i, customer_id in enumerate(customers, 1):
            logger.info(f"\nProcessing customer {i}/{len(customers)}: {customer_id}")
            stats = audit_customer(customer_id, dry_run)
            all_stats.append(stats)

    # Summary
    logger.info("\n" + "="*80)
    logger.info("GRAND TOTAL")
    logger.info("="*80)
    total_done_labels = sum(s['ad_groups_with_done_label'] for s in all_stats)
    total_missing_bf = sum(s['ad_groups_missing_bf_ad'] for s in all_stats)
    total_removed = sum(s['labels_removed'] for s in all_stats)
    total_errors = sum(s['errors'] for s in all_stats)

    logger.info(f"Customers processed: {len(all_stats)}")
    logger.info(f"Ad groups with THEME_BF_DONE: {total_done_labels}")
    logger.info(f"Ad groups missing BF ads: {total_missing_bf}")
    if not dry_run:
        logger.info(f"Labels removed: {total_removed}")
    else:
        logger.info(f"Labels that would be removed: {total_missing_bf}")
    logger.info(f"Errors: {total_errors}")
    logger.info("="*80)


if __name__ == '__main__':
    main()
