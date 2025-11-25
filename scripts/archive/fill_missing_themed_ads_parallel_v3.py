"""
Find all ad groups with THEME_*_DONE labels and create missing themed ads.
V3 - Fixed with:
  - Themed headlines/descriptions from themes directory
  - Proper labeling of ads and ad groups
  - Batch ad creation
  - Progress persistence
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
import logging
import psycopg2
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import json
from typing import Dict, List, Set, Tuple
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

# Theme configuration (excluding singles_day)
THEMES = {
    'THEME_BF': 'black_friday',
    'THEME_CM': 'cyber_monday',
    'THEME_SK': 'sinterklaas',
    'THEME_KM': 'kerstmis'
}

# Progress file for tracking completed customers
PROGRESS_FILE = Path(__file__).parent / "fill_missing_progress_v3.json"

# Batch size for API operations
BATCH_SIZE = 100


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
            return row.label.resource_name

        # Create label if it doesn't exist
        label_service = client.get_service('LabelService')
        operation = client.get_type('LabelOperation')
        label = operation.create
        label.name = label_name

        response = label_service.mutate_labels(
            customer_id=customer_id,
            operations=[operation]
        )

        return response.results[0].resource_name

    except Exception as e:
        logger.warning(f"Label operation warning for {label_name}: {e}")
        # Return a dummy resource name to continue
        return f"customers/{customer_id}/labels/dummy"


def check_and_add_all_themes_done(client, customer_id: str, ad_group_id: str, dry_run: bool = True):
    """
    Check if all 4 main themes (BF, CM, SK, KM) have DONE labels on the ad group.
    If so, add the ALL_THEMES_DONE label.
    """
    if dry_run:
        return

    try:
        ga_service = client.get_service('GoogleAdsService')

        # Query to get all labels on this ad group
        query = f"""
            SELECT
                label.name
            FROM ad_group_label
            WHERE ad_group_label.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
            AND label.name LIKE 'THEME_%_DONE'
        """

        response = ga_service.search(customer_id=customer_id, query=query)

        # Collect all DONE labels on this ad group
        labels_on_ad_group = set()
        for row in response:
            labels_on_ad_group.add(row.label.name)

        # Check if all 4 main themes are present
        required_labels = {'THEME_BF_DONE', 'THEME_CM_DONE', 'THEME_SK_DONE', 'THEME_KM_DONE'}

        if required_labels.issubset(labels_on_ad_group):
            # All 4 themes are present, add ALL_THEMES_DONE label
            logger.info(f"    ✓ All 4 themes complete, adding ALL_THEMES_DONE label")

            # Get or create ALL_THEMES_DONE label
            all_done_label_resource = get_or_create_label(client, customer_id, 'ALL_THEMES_DONE')

            # Add label to ad group
            ag_label_service = client.get_service('AdGroupLabelService')
            ag_operation = client.get_type('AdGroupLabelOperation')
            ag_label = ag_operation.create
            ag_label.ad_group = f'customers/{customer_id}/adGroups/{ad_group_id}'
            ag_label.label = all_done_label_resource

            ag_label_service.mutate_ad_group_labels(
                customer_id=customer_id,
                operations=[ag_operation]
            )

    except Exception as e:
        # Don't fail if ALL_THEMES_DONE label can't be added
        if 'ENTITY_ALREADY_EXISTS' not in str(e):
            logger.warning(f"    Warning adding ALL_THEMES_DONE label: {e}")


def create_themed_ads_batch(client, customer_id: str, ad_group_id: str,
                           base_ad, missing_themes: List[Tuple[str, str]],
                           dry_run: bool = True) -> Tuple[int, List[Tuple[str, str, str]]]:
    """
    Create multiple themed ads in a single batch operation with themed content.

    Returns:
        Tuple of (num_created, list_of_(resource_name, theme_label, theme_name))
    """
    if not missing_themes:
        return 0, []

    try:
        # Load themed content for each theme
        from themes import load_theme_content

        operations = []
        theme_info_list = []

        # Create one operation for each missing theme
        for theme_label, theme_name in missing_themes:
            # Load themed headlines and descriptions
            theme_content = load_theme_content(theme_name)

            ad_group_ad_operation = client.get_type('AdGroupAdOperation')
            new_ad_group_ad = ad_group_ad_operation.create
            new_ad_group_ad.ad_group = f'customers/{customer_id}/adGroups/{ad_group_id}'
            new_ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED

            # Create RSA with themed content
            rsa = new_ad_group_ad.ad.responsive_search_ad

            # Set path1 to theme name
            rsa.path1 = theme_name

            # Add themed headlines (max 15, Google Ads will select best ones)
            for headline_text in theme_content.headlines[:15]:
                h = client.get_type('AdTextAsset')
                h.text = headline_text  # Already within 30 char limit from theme files
                rsa.headlines.append(h)

            # Add themed descriptions (max 4 for RSA)
            for desc_text in theme_content.descriptions[:4]:
                d = client.get_type('AdTextAsset')
                d.text = desc_text  # Already within 90 char limit from theme files
                rsa.descriptions.append(d)

            # Set final URLs with campaign_theme=1
            for url in base_ad.final_urls:
                from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
                parsed = urlparse(url)
                params = parse_qs(parsed.query)
                params['campaign_theme'] = ['1']  # Add campaign_theme=1 parameter
                new_query = urlencode(params, doseq=True)
                new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path,
                                     parsed.params, new_query, parsed.fragment))
                new_ad_group_ad.ad.final_urls.append(new_url)

            operations.append(ad_group_ad_operation)
            theme_info_list.append((theme_label, theme_name))

        if dry_run:
            logger.info(f"    [DRY RUN] Would create {len(operations)} themed ads in batch")
            return len(operations), []

        # Execute batch operation
        ad_group_ad_service = client.get_service('AdGroupAdService')
        response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=customer_id,
            operations=operations
        )

        results = []
        for i, result in enumerate(response.results):
            theme_label, theme_name = theme_info_list[i]
            results.append((result.resource_name, theme_label, theme_name))

        logger.info(f"    ✓ Created {len(results)} themed ads in batch")
        return len(results), results

    except Exception as e:
        logger.error(f"    Error creating themed ads batch: {e}")
        return 0, []


def add_labels_to_ads(client, customer_id: str, ad_group_id: str,
                      ad_resources: List[Tuple[str, str, str]], dry_run: bool = True):
    """
    Add theme labels to created ads and DONE labels to ad group.

    Args:
        ad_resources: List of (resource_name, theme_label, theme_name) tuples
    """
    if dry_run or not ad_resources:
        return

    try:
        # Add labels to each ad
        for resource_name, theme_label, theme_name in ad_resources:
            try:
                # Get or create the theme label (e.g., THEME_BF)
                label_resource = get_or_create_label(client, customer_id, theme_label)

                # Extract ad ID from resource name (format: customers/123/adGroupAds/456~789)
                ad_id = resource_name.split('~')[-1]

                # Add label to ad
                ad_label_service = client.get_service('AdGroupAdLabelService')
                operation = client.get_type('AdGroupAdLabelOperation')
                ad_label = operation.create
                ad_label.ad_group_ad = resource_name
                ad_label.label = label_resource

                ad_label_service.mutate_ad_group_ad_labels(
                    customer_id=customer_id,
                    operations=[operation]
                )

                # Add DONE label to ad group (e.g., THEME_BF_DONE)
                done_label = f"{theme_label}_DONE"
                done_label_resource = get_or_create_label(client, customer_id, done_label)

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
                # Continue even if labeling fails (ads are still created)
                if 'ENTITY_ALREADY_EXISTS' not in str(e):
                    logger.warning(f"    Label warning for {theme_label}: {e}")

        # Check if all 4 themes are now complete and add ALL_THEMES_DONE label
        check_and_add_all_themes_done(client, customer_id, ad_group_id, dry_run)

    except Exception as e:
        logger.warning(f"    Error in label operations: {e}")


def process_single_customer(args):
    """Process a single customer to find and fill missing themed ads."""
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

        # Process ad groups
        ad_group_items = list(ad_groups.items())
        processed_count = 0

        for ag_id, ag_info in ad_group_items:
            existing_labels = ag_info['labels']
            missing_themes = []

            for theme_label, theme_name in THEMES.items():
                done_label = f"{theme_label}_DONE"
                if done_label not in existing_labels:
                    missing_themes.append((theme_label, theme_name))

            if not missing_themes:
                continue

            # Log every 10th ad group to reduce noise
            if processed_count % 10 == 0 or processed_count < 3:
                logger.info(f"\n  Ad Group [{processed_count+1}/{len(ad_group_items)}]: {ag_info['name'][:60]}")
                logger.info(f"    ID: {ag_id}")
                logger.info(f"    Missing themes: {len(missing_themes)}")

            # Get a base ad from this ad group
            ad_query = f"""
                SELECT
                    ad_group_ad.ad.id,
                    ad_group_ad.ad.responsive_search_ad.headlines,
                    ad_group_ad.ad.responsive_search_ad.descriptions,
                    ad_group_ad.ad.final_urls
                FROM ad_group_ad
                WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ag_id}'
                    AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                    AND ad_group_ad.status = ENABLED
                LIMIT 1
            """

            ad_response = ga_service.search(customer_id=customer_id, query=ad_query)
            base_ad = None
            for row in ad_response:
                base_ad = row.ad_group_ad.ad
                break

            if not base_ad:
                logger.warning(f"    No base ad found, skipping")
                continue

            # Create all missing themed ads in a single batch
            num_created, ad_resources = create_themed_ads_batch(
                client, customer_id, ag_id, base_ad, missing_themes, dry_run
            )

            if num_created > 0:
                result['ads_created'] += num_created
                result['ad_groups_processed'] += 1

                # Add labels to the created ads
                add_labels_to_ads(client, customer_id, ag_id, ad_resources, dry_run)

            processed_count += 1

        result['success'] = True
        logger.info(f"\n  Customer {customer_id} Summary: Processed {result['ad_groups_processed']} ad groups, "
                   f"created {result['ads_created']} themed ads")

    except Exception as e:
        logger.error(f"  Failed to process customer {customer_id}: {e}", exc_info=True)
        result['error'] = str(e)

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Fill missing themed ads - V3 with themed content and labeling'
    )
    parser.add_argument('--execute', action='store_true',
                       help='Actually create ads (default is dry-run)')
    parser.add_argument('--customer-limit', type=int,
                       help='Limit number of customers to process (for testing)')
    parser.add_argument('--parallel', type=int, default=3,
                       help='Number of customers to process in parallel (default: 3)')
    parser.add_argument('--reset-progress', action='store_true',
                       help='Reset progress and start from scratch')
    parser.add_argument('--skip-completed', action='store_true', default=True,
                       help='Skip already completed customers (default: True)')

    args = parser.parse_args()

    dry_run = not args.execute

    # Load progress
    progress = load_progress()
    completed_customers = set(progress['completed_customers'])

    if args.reset_progress:
        logger.info("Resetting progress...")
        completed_customers = set()
        save_progress([])

    if dry_run:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info(f"EXECUTE MODE (PARALLEL: {args.parallel} workers, BATCH SIZE: {BATCH_SIZE})")
        logger.info("=" * 80)
        response = input("Are you sure you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborted")
            return

    # Get all customer IDs
    logger.info("Fetching customer IDs from database...")
    all_customer_ids = get_all_customer_ids()

    # Filter out completed customers if requested
    if args.skip_completed and not dry_run:
        customer_ids = [c for c in all_customer_ids if c not in completed_customers]
        logger.info(f"Found {len(all_customer_ids)} total customers")
        logger.info(f"Already completed: {len(completed_customers)}")
        logger.info(f"Remaining to process: {len(customer_ids)}")
    else:
        customer_ids = all_customer_ids
        logger.info(f"Found {len(customer_ids)} customers to process")

    if args.customer_limit:
        customer_ids = customer_ids[:args.customer_limit]
        logger.info(f"Limited to first {args.customer_limit} customers")

    if not customer_ids:
        logger.info("No customers to process!")
        return

    logger.info(f"Using {args.parallel} parallel workers")
    logger.info(f"Batch size: {BATCH_SIZE} operations per API call")
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
    newly_completed = []
    completed_count = 0

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
            completed_count += 1

            try:
                result = future.result()

                if result['success']:
                    total_ad_groups += result['ad_groups_processed']
                    total_ads_created += result['ads_created']
                    newly_completed.append(customer_id)

                    # Save progress after each successful customer
                    if not dry_run:
                        all_completed = list(completed_customers) + newly_completed
                        save_progress(all_completed)

                    logger.info(f"✓ Completed customer {result['customer_id']} ({completed_count}/{len(customer_ids)}) - "
                               f"{result['ad_groups_processed']} ad groups, {result['ads_created']} ads created")
                else:
                    failed_customers.append((result['customer_id'], result['error']))
                    logger.error(f"✗ Failed customer {result['customer_id']} ({completed_count}/{len(customer_ids)})")

            except Exception as e:
                logger.error(f"✗ Exception processing customer {customer_id}: {e}")
                failed_customers.append((customer_id, str(e)))

    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total customers processed: {len(customer_ids)}")
    logger.info(f"Successfully completed: {len(newly_completed)}")
    logger.info(f"Total ad groups with missing themes: {total_ad_groups}")
    logger.info(f"Total themed ads {'would be created' if dry_run else 'created'}: {total_ads_created}")

    if failed_customers:
        logger.info(f"\nFailed customers ({len(failed_customers)}):")
        for customer_id, error in failed_customers:
            logger.info(f"  - {customer_id}: {error}")

    if not dry_run and progress['last_updated']:
        logger.info(f"\nProgress saved to: {PROGRESS_FILE}")
        logger.info(f"Total completed customers: {len(completed_customers) + len(newly_completed)}")

    logger.info("=" * 80)


if __name__ == '__main__':
    main()
