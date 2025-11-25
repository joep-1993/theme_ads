#!/usr/bin/env python3
"""
Create missing Black Friday ads in all ad groups.
Labels ad groups with DESTINATION_NOT_WORKING when policy errors occur.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
import logging
import psycopg2
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import json
from typing import List, Tuple
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Add paths
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))
sys.path.insert(0, str(Path(__file__).parent))

# Progress file
PROGRESS_FILE = Path(__file__).parent / "black_friday_progress.json"


def load_progress():
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {'completed_customers': [], 'last_updated': None}
    return {'completed_customers': [], 'last_updated': None}


def save_progress(completed_customers: List[str]):
    """Save progress to file."""
    progress = {
        'completed_customers': completed_customers,
        'last_updated': datetime.now().isoformat()
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def get_all_customer_ids():
    """Get all customer IDs from the database."""
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
    customer_ids = [row[0] for row in cur.fetchall()]
    conn.close()
    return customer_ids


def get_or_create_label(client, customer_id: str, label_name: str) -> str:
    """Get or create a label in Google Ads."""
    try:
        # Search for existing label
        ga_service = client.get_service('GoogleAdsService')
        query = f"""
            SELECT label.resource_name, label.name
            FROM label
            WHERE label.name = '{label_name}'
            LIMIT 1
        """
        response = ga_service.search(customer_id=customer_id, query=query)

        for row in response:
            logger.debug(f"Found existing label '{label_name}': {row.label.resource_name}")
            return row.label.resource_name

        # Create label if it doesn't exist
        label_service = client.get_service('LabelService')
        label_operation = client.get_type('LabelOperation')
        label = label_operation.create
        label.name = label_name

        response = label_service.mutate_labels(
            customer_id=customer_id,
            operations=[label_operation]
        )

        logger.debug(f"Created label '{label_name}': {response.results[0].resource_name}")
        return response.results[0].resource_name

    except Exception as e:
        logger.error(f"Error getting/creating label '{label_name}': {e}")
        raise


def find_ad_groups_needing_black_friday(client, customer_id: str):
    """Find ad groups that have theme labels but missing Black Friday ads."""
    ga_service = client.get_service('GoogleAdsService')

    # First, get all ad groups with THEME_*_DONE labels
    query = """
        SELECT
            ad_group.id,
            ad_group.name,
            campaign.id,
            campaign.name
        FROM ad_group_label
        WHERE label.name LIKE 'THEME_%_DONE'
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=query)

        ad_groups_with_themes = set()
        for row in response:
            ad_group_id = row.ad_group.id
            ad_groups_with_themes.add(ad_group_id)

        # Now get ad groups with DESTINATION_NOT_WORKING label to exclude
        exclude_query = """
            SELECT
                ad_group.id
            FROM ad_group_label
            WHERE label.name = 'DESTINATION_NOT_WORKING'
        """

        exclude_response = ga_service.search(customer_id=customer_id, query=exclude_query)
        excluded_ad_groups = set()
        for row in exclude_response:
            excluded_ad_groups.add(row.ad_group.id)

        # Filter out excluded ad groups
        ad_groups_with_themes -= excluded_ad_groups

        logger.info(f"Found {len(ad_groups_with_themes)} ad groups with theme labels (excluding {len(excluded_ad_groups)} with DESTINATION_NOT_WORKING)")

        if not ad_groups_with_themes:
            return []

        # Now check which of these are missing Black Friday ads
        ad_groups_missing_bf = []

        for ad_group_id in ad_groups_with_themes:
            # Check if Black Friday ad exists
            bf_check_query = f"""
                SELECT ad_group_ad.ad.id
                FROM ad_group_ad
                WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
                AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.ad.responsive_search_ad.path1 = 'black_friday'
                LIMIT 1
            """

            bf_response = ga_service.search(customer_id=customer_id, query=bf_check_query)
            has_bf_ad = sum(1 for _ in bf_response) > 0

            if not has_bf_ad:
                # Get ad group details
                ag_query = f"""
                    SELECT
                        ad_group.id,
                        ad_group.name,
                        campaign.id,
                        campaign.name
                    FROM ad_group
                    WHERE ad_group.id = {ad_group_id}
                """
                ag_response = ga_service.search(customer_id=customer_id, query=ag_query)
                for row in ag_response:
                    ad_groups_missing_bf.append({
                        'ad_group_id': str(ad_group_id),
                        'ad_group_name': row.ad_group.name,
                        'campaign_id': str(row.campaign.id),
                        'campaign_name': row.campaign.name
                    })
                    break

        logger.info(f"Found {len(ad_groups_missing_bf)} ad groups missing Black Friday ads")
        return ad_groups_missing_bf

    except Exception as e:
        logger.error(f"Error finding ad groups: {e}")
        return []


def get_base_ad_for_ad_group(client, customer_id: str, ad_group_id: str):
    """Get a base ad from the ad group to copy URLs from."""
    ga_service = client.get_service('GoogleAdsService')

    query = f"""
        SELECT
            ad_group_ad.ad.final_urls
        FROM ad_group_ad
        WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
        AND ad_group_ad.status = ENABLED
        LIMIT 1
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            return row.ad_group_ad.ad
        return None
    except Exception as e:
        logger.error(f"Error getting base ad: {e}")
        return None


def create_black_friday_ad(client, customer_id: str, ad_group_id: str, base_ad, dry_run: bool = True) -> Tuple[bool, str]:
    """
    Create a Black Friday ad for an ad group.

    Returns:
        Tuple of (success: bool, error_type: str)
        error_type can be: 'success', 'destination_not_working', 'other_error'
    """
    try:
        from themes import load_theme_content

        # Load Black Friday content
        theme_content = load_theme_content('black_friday')

        ad_group_ad_operation = client.get_type('AdGroupAdOperation')
        new_ad_group_ad = ad_group_ad_operation.create
        new_ad_group_ad.ad_group = f'customers/{customer_id}/adGroups/{ad_group_id}'
        new_ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED

        # Create RSA with themed content
        rsa = new_ad_group_ad.ad.responsive_search_ad

        # Set path1 to black_friday
        rsa.path1 = 'black_friday'

        # Add themed headlines (max 15)
        for headline_text in theme_content.headlines[:15]:
            h = client.get_type('AdTextAsset')
            h.text = headline_text
            rsa.headlines.append(h)

        # Add themed descriptions (max 4)
        for desc_text in theme_content.descriptions[:4]:
            d = client.get_type('AdTextAsset')
            d.text = desc_text
            rsa.descriptions.append(d)

        # Set final URLs with campaign_theme=1
        for url in base_ad.final_urls:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            params['campaign_theme'] = ['1']
            new_query = urlencode(params, doseq=True)
            new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path,
                                 parsed.params, new_query, parsed.fragment))
            new_ad_group_ad.ad.final_urls.append(new_url)

        if dry_run:
            logger.info(f"    [DRY RUN] Would create Black Friday ad")
            return True, 'success'

        # Execute operation
        ad_group_ad_service = client.get_service('AdGroupAdService')
        response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=customer_id,
            operations=[ad_group_ad_operation]
        )

        ad_resource = response.results[0].resource_name

        # Add labels to ad
        try:
            label_resource = get_or_create_label(client, customer_id, 'THEME_BF')
            ad_label_service = client.get_service('AdGroupAdLabelService')
            operation = client.get_type('AdGroupAdLabelOperation')
            ad_label = operation.create
            ad_label.ad_group_ad = ad_resource
            ad_label.label = label_resource

            ad_label_service.mutate_ad_group_ad_labels(
                customer_id=customer_id,
                operations=[operation]
            )

            # Add THEME_BF_DONE label to ad group
            done_label_resource = get_or_create_label(client, customer_id, 'THEME_BF_DONE')
            ag_label_service = client.get_service('AdGroupLabelService')
            ag_operation = client.get_type('AdGroupLabelOperation')
            ag_label = ag_operation.create
            ag_label.ad_group = f'customers/{customer_id}/adGroups/{ad_group_id}'
            ag_label.label = done_label_resource

            ag_label_service.mutate_ad_group_labels(
                customer_id=customer_id,
                operations=[ag_operation]
            )
        except Exception as e:
            if 'ENTITY_ALREADY_EXISTS' not in str(e):
                logger.warning(f"    Label warning: {e}")

        logger.info(f"    ✓ Created Black Friday ad")
        return True, 'success'

    except Exception as e:
        error_str = str(e)
        if 'DESTINATION_NOT_WORKING' in error_str:
            logger.warning(f"    Policy error: DESTINATION_NOT_WORKING")
            return False, 'destination_not_working'
        else:
            logger.error(f"    Error creating ad: {e}")
            return False, 'other_error'


def label_ad_group_destination_not_working(client, customer_id: str, ad_group_id: str, dry_run: bool = True):
    """Add DESTINATION_NOT_WORKING label to ad group."""
    if dry_run:
        logger.info(f"    [DRY RUN] Would label ad group with DESTINATION_NOT_WORKING")
        return

    try:
        label_resource = get_or_create_label(client, customer_id, 'DESTINATION_NOT_WORKING')
        ag_label_service = client.get_service('AdGroupLabelService')
        operation = client.get_type('AdGroupLabelOperation')
        ag_label = operation.create
        ag_label.ad_group = f'customers/{customer_id}/adGroups/{ad_group_id}'
        ag_label.label = label_resource

        ag_label_service.mutate_ad_group_labels(
            customer_id=customer_id,
            operations=[operation]
        )
        logger.info(f"    ✓ Labeled ad group with DESTINATION_NOT_WORKING")
    except Exception as e:
        if 'ENTITY_ALREADY_EXISTS' not in str(e):
            logger.warning(f"    Error labeling ad group: {e}")


def process_single_customer(args):
    """Process a single customer to create Black Friday ads."""
    customer_id, idx, total_customers, dry_run = args

    from google.ads.googleads.client import GoogleAdsClient

    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"Customer {idx}/{total_customers}: {customer_id}")
        logger.info(f"{'='*80}")

        # Initialize Google Ads client from environment
        config = {
            'developer_token': os.environ.get('GOOGLE_DEVELOPER_TOKEN'),
            'client_id': os.environ.get('GOOGLE_CLIENT_ID'),
            'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET'),
            'refresh_token': os.environ.get('GOOGLE_REFRESH_TOKEN'),
            'login_customer_id': os.environ.get('GOOGLE_LOGIN_CUSTOMER_ID'),
            'use_proto_plus': True
        }
        client = GoogleAdsClient.load_from_dict(config)

        # Find ad groups needing Black Friday ads
        ad_groups = find_ad_groups_needing_black_friday(client, customer_id)

        if not ad_groups:
            logger.info(f"No ad groups need Black Friday ads")
            return (customer_id, 0, 0, 0)

        created_count = 0
        destination_error_count = 0
        other_error_count = 0

        for ag in ad_groups:
            ad_group_id = ag['ad_group_id']
            ad_group_name = ag['ad_group_name']
            campaign_name = ag['campaign_name']

            logger.info(f"\n  Ad Group: {ad_group_name}")
            logger.info(f"  Campaign: {campaign_name}")
            logger.info(f"  Ad Group ID: {ad_group_id}")

            # Get base ad for URLs
            base_ad = get_base_ad_for_ad_group(client, customer_id, ad_group_id)
            if not base_ad or not base_ad.final_urls:
                logger.warning(f"  ✗ No base ad with URLs found, skipping")
                other_error_count += 1
                continue

            # Create Black Friday ad
            success, error_type = create_black_friday_ad(
                client, customer_id, ad_group_id, base_ad, dry_run
            )

            if success:
                created_count += 1
            elif error_type == 'destination_not_working':
                destination_error_count += 1
                # Label the ad group to skip it next time
                label_ad_group_destination_not_working(client, customer_id, ad_group_id, dry_run)
            else:
                other_error_count += 1

        logger.info(f"\n{'='*80}")
        logger.info(f"Customer {customer_id} Summary:")
        logger.info(f"  Ad Groups processed: {len(ad_groups)}")
        logger.info(f"  Black Friday ads created: {created_count}")
        logger.info(f"  Destination not working errors: {destination_error_count}")
        logger.info(f"  Other errors: {other_error_count}")
        logger.info(f"{'='*80}")

        return (customer_id, created_count, destination_error_count, other_error_count)

    except Exception as e:
        logger.error(f"Error processing customer {customer_id}: {e}")
        return (customer_id, 0, 0, 1)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Create missing Black Friday ads')
    parser.add_argument('--execute', action='store_true', help='Execute creation (default is dry-run)')
    parser.add_argument('--parallel', type=int, default=3, help='Number of parallel workers (default: 3)')
    parser.add_argument('--customer-limit', type=int, help='Limit to first N customers (for testing)')
    parser.add_argument('--reset-progress', action='store_true', help='Reset progress and start from beginning')
    args = parser.parse_args()

    dry_run = not args.execute

    logger.info("="*80)
    logger.info(f"{'DRY RUN' if dry_run else 'EXECUTE'} MODE (PARALLEL: {args.parallel} workers)")
    logger.info("="*80)

    if not dry_run:
        confirm = input("Are you sure you want to CREATE Black Friday ads? (yes/no): ")
        if confirm.lower() != 'yes':
            logger.info("Aborted by user")
            return

    # Load progress
    progress = load_progress()
    if args.reset_progress:
        progress['completed_customers'] = []
        logger.info("Progress reset")

    completed_customers = set(progress['completed_customers'])

    # Get customer IDs
    customer_ids = get_all_customer_ids()
    if not customer_ids:
        logger.error("No customer IDs found")
        return

    # Filter out completed customers
    customer_ids = [cid for cid in customer_ids if cid not in completed_customers]

    if args.customer_limit:
        customer_ids = customer_ids[:args.customer_limit]
        logger.info(f"Limited to first {args.customer_limit} customers")

    logger.info(f"Processing {len(customer_ids)} customers with {args.parallel} parallel workers")

    # Process in parallel
    results = []
    with ProcessPoolExecutor(max_workers=args.parallel) as executor:
        futures = {
            executor.submit(
                process_single_customer,
                (cid, idx+1, len(customer_ids), dry_run)
            ): cid
            for idx, cid in enumerate(customer_ids)
        }

        for future in as_completed(futures):
            customer_id, created, dest_errors, other_errors = future.result()
            results.append((customer_id, created, dest_errors, other_errors))
            completed_customers.add(customer_id)

            # Save progress
            if not dry_run:
                save_progress(list(completed_customers))

            logger.info(f"✓ Completed customer {customer_id} - Created: {created}, Dest errors: {dest_errors}, Other errors: {other_errors}")

    # Summary
    logger.info("\n" + "="*80)
    logger.info("GRAND TOTAL")
    logger.info("="*80)

    total_created = sum(r[1] for r in results)
    total_dest_errors = sum(r[2] for r in results)
    total_other_errors = sum(r[3] for r in results)

    logger.info(f"Total Black Friday ads created: {total_created}")
    logger.info(f"Total destination not working errors: {total_dest_errors}")
    logger.info(f"Total other errors: {total_other_errors}")
    logger.info(f"Customers processed: {len(results)}")


if __name__ == '__main__':
    main()
