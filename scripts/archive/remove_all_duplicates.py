"""
Remove duplicate RSAs from all customers that have had ads created.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
import logging
import psycopg2

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Initialize Google Ads client
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))
sys.path.insert(0, str(Path(__file__).parent))
from remove_duplicate_ads import find_duplicate_ads, remove_duplicate_ads
from config import load_config_from_env
from google_ads_client import initialize_client

config = load_config_from_env()
client = initialize_client(config.google_ads)


def get_all_customer_ids():
    """Get all customer IDs from the database."""
    import os
    # Use DATABASE_URL directly
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


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Remove duplicate RSAs from all customers')
    parser.add_argument('--execute', action='store_true', help='Actually remove ads (default is dry-run)')
    parser.add_argument('--customer-limit', type=int, help='Limit number of customers to process (for testing)')

    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info("EXECUTE MODE - Will remove duplicate ads!")
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

    # Process each customer
    total_removed = 0
    customers_with_duplicates = 0
    failed_customers = []

    for idx, customer_id in enumerate(customer_ids, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"[{idx}/{len(customer_ids)}] Processing customer {customer_id}")
        logger.info(f"{'='*80}")

        try:
            # Find duplicates
            duplicates_by_ag = find_duplicate_ads(customer_id, limit=None, skip_labeled=True)

            if not duplicates_by_ag:
                logger.info(f"  No duplicates found for customer {customer_id}")
                continue

            customers_with_duplicates += 1

            # Remove duplicates
            removed_count = remove_duplicate_ads(customer_id, duplicates_by_ag, dry_run=dry_run, add_labels=True)
            total_removed += removed_count

            logger.info(f"  {'Would remove' if dry_run else 'Removed'} {removed_count} duplicate ads for customer {customer_id}")

        except Exception as e:
            logger.error(f"  Failed to process customer {customer_id}: {e}", exc_info=True)
            failed_customers.append((customer_id, str(e)))
            continue

    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total customers processed: {len(customer_ids)}")
    logger.info(f"Customers with duplicates: {customers_with_duplicates}")
    logger.info(f"Total duplicate ads {'would be removed' if dry_run else 'removed'}: {total_removed}")

    if failed_customers:
        logger.info(f"\nFailed customers ({len(failed_customers)}):")
        for customer_id, error in failed_customers:
            logger.info(f"  - {customer_id}: {error}")

    logger.info("=" * 80)


if __name__ == '__main__':
    main()
