"""
Pause all ENABLED themed ads (those with themed URLs in path1).
This will fix ads that were created with ENABLED status before the bug was fixed.
PARALLEL VERSION - processes multiple customers simultaneously.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
import logging
import psycopg2
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

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

# Theme keywords to detect in URLs
THEME_KEYWORDS = ['black_friday', 'cyber_monday', 'sinterklaas', 'kerstmis']


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


def process_single_customer(args):
    """
    Process a single customer to find and pause ENABLED themed ads.

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
        'ads_found': 0,
        'ads_paused': 0,
        'success': False,
        'error': None,
        'idx': idx
    }

    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"[{idx}/{total_customers}] Processing customer {customer_id}")
        logger.info(f"{'='*80}")

        # Find all ENABLED RSAs with themed URLs
        query = """
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.ad.final_urls
            FROM ad_group_ad
            WHERE ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.status = ENABLED
        """

        response = ga_service.search(customer_id=customer_id, query=query)

        ads_to_pause = []

        for row in response:
            ad_group_id = str(row.ad_group.id)
            ad_id = str(row.ad_group_ad.ad.id)

            # Check if this ad has a themed URL
            has_themed_url = False
            themed_url = None
            for url in row.ad_group_ad.ad.final_urls:
                if any(theme in url.lower() for theme in THEME_KEYWORDS):
                    has_themed_url = True
                    themed_url = url
                    break

            if has_themed_url:
                result['ads_found'] += 1
                ads_to_pause.append({
                    'ad_group_id': ad_group_id,
                    'ad_group_name': row.ad_group.name,
                    'ad_id': ad_id,
                    'url': themed_url
                })

        if not ads_to_pause:
            logger.info(f"  No ENABLED themed ads found")
            result['success'] = True
            return result

        logger.info(f"  Found {len(ads_to_pause)} ENABLED themed ads to pause")

        # Pause the ads
        if not dry_run:
            ad_group_ad_service = client.get_service('AdGroupAdService')
            operations = []

            for ad_info in ads_to_pause:
                operation = client.get_type('AdGroupAdOperation')
                ad_group_ad = operation.update
                ad_group_ad.resource_name = f"customers/{customer_id}/adGroupAds/{ad_info['ad_group_id']}~{ad_info['ad_id']}"
                ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED

                # Set field mask directly (API v22+ compatible)
                operation.update_mask.paths.append('status')

                operations.append(operation)

                # Show first few examples
                if result['ads_paused'] < 10:
                    logger.info(f"    Pausing ad {ad_info['ad_id']} in {ad_info['ad_group_name'][:50]}")

            # Execute in batches of 100
            batch_size = 100
            for i in range(0, len(operations), batch_size):
                batch = operations[i:i+batch_size]
                try:
                    ad_group_ad_service.mutate_ad_group_ads(
                        customer_id=customer_id,
                        operations=batch
                    )
                    result['ads_paused'] += len(batch)
                except Exception as e:
                    logger.error(f"    Error pausing batch: {e}")

            logger.info(f"  ✓ Paused {result['ads_paused']} themed ads")
        else:
            logger.info(f"  [DRY RUN] Would pause {len(ads_to_pause)} themed ads")
            result['ads_paused'] = len(ads_to_pause)

        result['success'] = True

    except Exception as e:
        logger.error(f"  Failed to process customer {customer_id}: {e}", exc_info=True)
        result['error'] = str(e)

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Pause ENABLED themed ads (PARALLEL)'
    )
    parser.add_argument('--execute', action='store_true',
                       help='Actually pause ads (default is dry-run)')
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
        logger.info(f"EXECUTE MODE (PARALLEL: {args.parallel} workers) - Will pause themed ads!")
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

    # Prepare arguments for parallel processing
    customer_args = [
        (customer_id, idx, len(customer_ids), dry_run)
        for idx, customer_id in enumerate(customer_ids, 1)
    ]

    # Process customers in parallel
    total_ads_found = 0
    total_ads_paused = 0
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
                    total_ads_found += result['ads_found']
                    total_ads_paused += result['ads_paused']
                    logger.info(f"✓ Completed customer {result['customer_id']} ({completed}/{len(customer_ids)}) - "
                               f"{result['ads_found']} found, {result['ads_paused']} paused")
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
    logger.info(f"Total ENABLED themed ads found: {total_ads_found}")
    logger.info(f"Total themed ads {'would be paused' if dry_run else 'paused'}: {total_ads_paused}")

    if failed_customers:
        logger.info(f"\nFailed customers ({len(failed_customers)}):")
        for customer_id, error in failed_customers:
            logger.info(f"  - {customer_id}: {error}")

    logger.info("=" * 80)


if __name__ == '__main__':
    main()
