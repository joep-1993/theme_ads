#!/usr/bin/env python3
"""
Delete SD_CHECKED labels from all accounts.
This simply deletes the label itself, which automatically removes it from all ad groups.
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add thema_ads_optimized to path
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))

from dotenv import load_dotenv
from config import load_config_from_env
from google_ads_client import initialize_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def delete_sd_checked_labels(customer_ids: list[str]):
    """Delete SD_CHECKED label from all accounts."""

    # Load environment
    env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
    load_dotenv(env_path)

    # Initialize
    config = load_config_from_env()
    client = initialize_client(config.google_ads)
    ga_service = client.get_service("GoogleAdsService")
    label_service = client.get_service("LabelService")

    total_deleted = 0

    for customer_id in customer_ids:
        logger.info(f"Processing customer {customer_id}")

        try:
            # Find SD_CHECKED label
            label_query = """
                SELECT label.resource_name, label.id, label.name
                FROM label
                WHERE label.name = 'SD_CHECKED'
                LIMIT 1
            """

            sd_checked_resource = None
            try:
                label_response = ga_service.search(customer_id=customer_id, query=label_query)
                for row in label_response:
                    sd_checked_resource = row.label.resource_name
                    logger.info(f"Customer {customer_id}: Found SD_CHECKED label: {sd_checked_resource}")
                    break
            except Exception as e:
                logger.warning(f"Customer {customer_id}: Could not find SD_CHECKED label: {e}")
                continue

            if not sd_checked_resource:
                logger.info(f"Customer {customer_id}: No SD_CHECKED label found, skipping")
                continue

            # Delete the label (this automatically removes it from all ad groups)
            try:
                operation = client.get_type("LabelOperation")
                operation.remove = sd_checked_resource

                response = label_service.mutate_labels(
                    customer_id=customer_id,
                    operations=[operation]
                )
                total_deleted += 1
                logger.info(f"Customer {customer_id}: Deleted SD_CHECKED label")
            except Exception as e:
                logger.error(f"Customer {customer_id}: Error deleting label: {e}")

        except Exception as e:
            logger.error(f"Customer {customer_id}: Unexpected error: {e}", exc_info=True)

    logger.info(f"Total SD_CHECKED labels deleted: {total_deleted}")


if __name__ == "__main__":
    # Get MCC customer IDs from config
    env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
    load_dotenv(env_path)

    import os
    mcc_id = os.getenv("GOOGLE_LOGIN_CUSTOMER_ID")

    if not mcc_id:
        logger.error("GOOGLE_LOGIN_CUSTOMER_ID not found in .env")
        sys.exit(1)

    # Initialize client to get all customer IDs
    config = load_config_from_env()
    client = initialize_client(config.google_ads)
    ga_service = client.get_service("GoogleAdsService")

    # Get all accessible customers
    customer_ids = []
    logger.info(f"Fetching customers from MCC {mcc_id}")

    query = """
        SELECT
            customer_client.id,
            customer_client.descriptive_name
        FROM customer_client
        WHERE customer_client.status = 'ENABLED'
        AND customer_client.manager = false
    """

    try:
        response = ga_service.search(customer_id=mcc_id, query=query)
        for row in response:
            customer_id = str(row.customer_client.id)
            customer_ids.append(customer_id)
            logger.info(f"Found customer: {customer_id} - {row.customer_client.descriptive_name}")
    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
        sys.exit(1)

    logger.info(f"Found {len(customer_ids)} customers")

    # Confirm before proceeding
    if len(sys.argv) > 1 and sys.argv[1] == '--confirm':
        logger.info("Running with --confirm flag, proceeding automatically")
    else:
        print(f"\nThis will DELETE the SD_CHECKED label from {len(customer_ids)} customers.")
        print("This will automatically remove the label from all ad groups.")
        confirm = input("Are you sure you want to proceed? (yes/no): ")

        if confirm.lower() != 'yes':
            logger.info("Operation cancelled")
            sys.exit(0)

    # Run the deletion
    delete_sd_checked_labels(customer_ids)
