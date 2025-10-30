"""
Find all ad groups with THEME_*_DONE labels and create missing themed ads.
Skips singles_day theme as requested.
PARALLEL VERSION - processes multiple customers simultaneously.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
import logging
import psycopg2
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from typing import Dict, List, Set, Tuple

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

# Theme configuration (excluding singles_day)
THEMES = {
    'THEME_BF': 'black_friday',
    'THEME_CM': 'cyber_monday',
    'THEME_SK': 'sinterklaas',
    'THEME_KM': 'kerstmis'
}

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


def create_themed_ad(client, customer_id: str, ad_group_id: str, theme_label: str, theme_name: str,
                     base_ad_id: str, dry_run: bool = True) -> Tuple[bool, str]:
    """
    Create a themed ad based on an existing ad.

    Returns:
        Tuple of (success: bool, ad_resource_name or error_message: str)
    """
    try:
        ga_service = client.get_service('GoogleAdsService')

        # Fetch the base ad details
        query = f"""
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions,
                ad_group_ad.ad.final_urls
            FROM ad_group_ad
            WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
                AND ad_group_ad.ad.id = {base_ad_id}
                AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
        """

        response = ga_service.search(customer_id=customer_id, query=query)
        base_ad = None
        for row in response:
            base_ad = row.ad_group_ad.ad
            break

        if not base_ad:
            return False, f"Base ad {base_ad_id} not found"

        # Create new ad with themed path1
        ad_group_ad_service = client.get_service('AdGroupAdService')
        ad_group_ad_operation = client.get_type('AdGroupAdOperation')

        new_ad_group_ad = ad_group_ad_operation.create
        new_ad_group_ad.ad_group = f'customers/{customer_id}/adGroups/{ad_group_id}'
        new_ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED

        # Copy RSA details
        rsa = new_ad_group_ad.ad.responsive_search_ad

        # Copy headlines
        for headline in base_ad.responsive_search_ad.headlines:
            h = client.get_type('AdTextAsset')
            h.text = headline.text
            if headline.pinned_field:
                h.pinned_field = headline.pinned_field
            rsa.headlines.append(h)

        # Copy descriptions
        for description in base_ad.responsive_search_ad.descriptions:
            d = client.get_type('AdTextAsset')
            d.text = description.text
            if description.pinned_field:
                d.pinned_field = description.pinned_field
            rsa.descriptions.append(d)

        # Set final URLs with themed path1
        for url in base_ad.final_urls:
            # Parse URL and add/update path1
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            params['path1'] = [theme_name]
            new_query = urlencode(params, doseq=True)
            new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path,
                                 parsed.params, new_query, parsed.fragment))
            new_ad_group_ad.ad.final_urls.append(new_url)

        if dry_run:
            logger.info(f"    [DRY RUN] Would create themed ad for {theme_name}")
            return True, "DRY_RUN"

        # Create the ad
        response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=customer_id,
            operations=[ad_group_ad_operation]
        )

        ad_resource = response.results[0].resource_name
        logger.info(f"    ✓ Created themed ad: {ad_resource}")
        return True, ad_resource

    except Exception as e:
        logger.error(f"    Error creating themed ad: {e}")
        return False, str(e)


def add_theme_label(client, customer_id: str, ad_group_id: str, label_name: str,
                    dry_run: bool = True) -> bool:
    """Add a theme DONE label to an ad group."""
    try:
        if dry_run:
            logger.info(f"    [DRY RUN] Would add label {label_name}")
            return True

        # Get or create the label
        from thema_ads_optimized.labels import get_or_create_label
        label_resource = get_or_create_label(client, customer_id, label_name)

        # Apply label to ad group
        ad_group_label_service = client.get_service('AdGroupLabelService')
        operation = client.get_type('AdGroupLabelOperation')

        ad_group_label = operation.create
        ad_group_label.ad_group = f'customers/{customer_id}/adGroups/{ad_group_id}'
        ad_group_label.label = label_resource

        ad_group_label_service.mutate_ad_group_labels(
            customer_id=customer_id,
            operations=[operation]
        )

        logger.info(f"    ✓ Added label {label_name}")
        return True

    except Exception as e:
        if 'ENTITY_ALREADY_EXISTS' in str(e) or 'already exists' in str(e).lower():
            logger.info(f"    Label {label_name} already exists")
            return True
        logger.error(f"    Error adding label {label_name}: {e}")
        return False


def add_theme_label_to_ad(client, customer_id: str, ad_id: str, ad_group_id: str,
                          label_name: str, dry_run: bool = True) -> bool:
    """Add a theme label to a specific ad."""
    try:
        if dry_run:
            return True

        from thema_ads_optimized.labels import get_or_create_label
        label_resource = get_or_create_label(client, customer_id, label_name)

        ad_label_service = client.get_service('AdGroupAdLabelService')
        operation = client.get_type('AdGroupAdLabelOperation')

        ad_label = operation.create
        ad_label.ad_group_ad = f'customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}'
        ad_label.label = label_resource

        ad_label_service.mutate_ad_group_ad_labels(
            customer_id=customer_id,
            operations=[operation]
        )

        return True

    except Exception as e:
        if 'ENTITY_ALREADY_EXISTS' not in str(e):
            logger.error(f"    Error adding ad label {label_name}: {e}")
        return True  # Continue even if label add fails


def process_single_customer(args):
    """
    Process a single customer to find and fill missing themed ads.

    Args:
        args: Tuple of (customer_id, idx, total_customers, dry_run)

    Returns:
        Dict with results
    """
    customer_id, idx, total_customers, dry_run = args

    # Import here to avoid issues with multiprocessing
    from config import load_config_from_env
    from google_ads_client import initialize_client

    config = load_config_from_env()
    client = initialize_client(config.google_ads)
    ga_service = client.get_service('GoogleAdsService')

    result = {
        'customer_id': customer_id,
        'ad_groups_processed': 0,
        'ads_created': 0,
        'success': False,
        'error': None,
        'idx': idx
    }

    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"[{idx}/{total_customers}] Processing customer {customer_id}")
        logger.info(f"{'='*80}")

        # Find all ad groups with at least one THEME_*_DONE label
        query = """
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group.campaign,
                label.name
            FROM ad_group_label
            WHERE label.name LIKE 'THEME_%_DONE'
                AND ad_group.status = ENABLED
        """

        response = ga_service.search(customer_id=customer_id, query=query)

        # Group by ad group
        ad_groups = {}
        for row in response:
            ag_id = str(row.ad_group.id)
            if ag_id not in ad_groups:
                ad_groups[ag_id] = {
                    'name': row.ad_group.name,
                    'campaign': row.ad_group.campaign,
                    'labels': set()
                }
            ad_groups[ag_id]['labels'].add(row.label.name)

        logger.info(f"Found {len(ad_groups)} ad groups with THEME_*_DONE labels")

        if not ad_groups:
            result['success'] = True
            return result

        # For each ad group, determine missing themes
        for ag_id, ag_info in ad_groups.items():
            existing_labels = ag_info['labels']
            missing_themes = []

            for theme_label, theme_name in THEMES.items():
                done_label = f"{theme_label}_DONE"
                if done_label not in existing_labels:
                    missing_themes.append((theme_label, theme_name, done_label))

            if not missing_themes:
                continue

            logger.info(f"\n  Ad Group: {ag_info['name'][:60]}")
            logger.info(f"    ID: {ag_id}")
            logger.info(f"    Existing: {sorted(existing_labels)}")
            logger.info(f"    Missing: {[t[0] for t in missing_themes]}")

            # Get a base ad from this ad group to copy
            ad_query = f"""
                SELECT
                    ad_group_ad.ad.id
                FROM ad_group_ad
                WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ag_id}'
                    AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                    AND ad_group_ad.status = ENABLED
                LIMIT 1
            """

            ad_response = ga_service.search(customer_id=customer_id, query=ad_query)
            base_ad_id = None
            for row in ad_response:
                base_ad_id = str(row.ad_group_ad.ad.id)
                break

            if not base_ad_id:
                logger.warning(f"    No base ad found, skipping")
                continue

            # Create missing themed ads
            for theme_label, theme_name, done_label in missing_themes:
                logger.info(f"    Creating ad for {theme_name}...")

                success, resource_or_error = create_themed_ad(
                    client, customer_id, ag_id, theme_label, theme_name,
                    base_ad_id, dry_run
                )

                if success:
                    result['ads_created'] += 1

                    # Add theme label to the new ad
                    if not dry_run and resource_or_error != "DRY_RUN":
                        # Extract ad ID from resource name
                        # Format: customers/123/adGroupAds/456~789
                        ad_id = resource_or_error.split('~')[-1]
                        add_theme_label_to_ad(client, customer_id, ad_id, ag_id,
                                             theme_label, dry_run)

                    # Add DONE label to ad group
                    add_theme_label(client, customer_id, ag_id, done_label, dry_run)
                else:
                    logger.error(f"    Failed: {resource_or_error}")

            result['ad_groups_processed'] += 1

        result['success'] = True
        logger.info(f"\n  Summary: Processed {result['ad_groups_processed']} ad groups, "
                   f"created {result['ads_created']} themed ads")

    except Exception as e:
        logger.error(f"  Failed to process customer {customer_id}: {e}", exc_info=True)
        result['error'] = str(e)

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Fill missing themed ads for ad groups with THEME_*_DONE labels (PARALLEL)'
    )
    parser.add_argument('--execute', action='store_true',
                       help='Actually create ads (default is dry-run)')
    parser.add_argument('--customer-limit', type=int,
                       help='Limit number of customers to process (for testing)')
    parser.add_argument('--parallel', type=int, default=3,
                       help='Number of customers to process in parallel (default: 3)')

    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info(f"EXECUTE MODE (PARALLEL: {args.parallel} workers) - Will create themed ads!")
        logger.info("=" * 80)
        response = input("Are you sure you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborted")
            return

    # Get all customer IDs
    logger.info("Fetching customer IDs from database...")
    customer_ids = get_all_customer_ids()

    if args.customer_limit:
        customer_ids = customer_ids[:args.customer_limit]

    logger.info(f"Found {len(customer_ids)} customers to process")
    logger.info(f"Using {args.parallel} parallel workers")
    logger.info(f"Themes to check: {', '.join(THEMES.keys())} (excluding THEME_SD)")

    # Prepare arguments for parallel processing
    customer_args = [
        (customer_id, idx, len(customer_ids), dry_run)
        for idx, customer_id in enumerate(customer_ids, 1)
    ]

    # Process customers in parallel
    total_ad_groups = 0
    total_ads_created = 0
    failed_customers = []
    completed = 0

    logger.info("\n" + "=" * 80)
    logger.info("Starting parallel processing...")
    logger.info("=" * 80)

    with ProcessPoolExecutor(max_workers=args.parallel) as executor:
        # Submit all jobs
        future_to_customer = {
            executor.submit(process_single_customer, arg): arg[0]
            for arg in customer_args
        }

        # Process results as they complete
        for future in as_completed(future_to_customer):
            customer_id = future_to_customer[future]
            completed += 1

            try:
                result = future.result()

                if result['success']:
                    total_ad_groups += result['ad_groups_processed']
                    total_ads_created += result['ads_created']
                    logger.info(f"✓ Completed customer {result['customer_id']} ({completed}/{len(customer_ids)}) - "
                               f"{result['ad_groups_processed']} ad groups, {result['ads_created']} ads created")
                else:
                    failed_customers.append((result['customer_id'], result['error']))
                    logger.error(f"✗ Failed customer {result['customer_id']} ({completed}/{len(customer_ids)})")

            except Exception as e:
                logger.error(f"✗ Exception processing customer {customer_id}: {e}")
                failed_customers.append((customer_id, str(e)))

    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total customers processed: {len(customer_ids)}")
    logger.info(f"Total ad groups with missing themes: {total_ad_groups}")
    logger.info(f"Total themed ads {'would be created' if dry_run else 'created'}: {total_ads_created}")

    if failed_customers:
        logger.info(f"\nFailed customers ({len(failed_customers)}):")
        for customer_id, error in failed_customers:
            logger.info(f"  - {customer_id}: {error}")

    logger.info("=" * 80)


if __name__ == '__main__':
    main()
