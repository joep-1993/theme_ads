#!/usr/bin/env python3
"""
Reset SD_CHECKED labels to allow check-up function to re-process ad groups.
This removes all SD_CHECKED labels from ad groups.
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


async def remove_sd_checked_labels(customer_ids: list[str]):
    """Remove all SD_CHECKED labels from ad groups."""

    # Load environment
    env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
    load_dotenv(env_path)

    # Initialize
    config = load_config_from_env()
    client = initialize_client(config.google_ads)
    ga_service = client.get_service("GoogleAdsService")
    ad_group_label_service = client.get_service("AdGroupLabelService")

    total_removed = 0

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

            # Find all ad group labels with SD_CHECKED
            ad_group_labels_query = f"""
                SELECT ad_group_label.resource_name, ad_group.id
                FROM ad_group_label
                WHERE ad_group_label.label = '{sd_checked_resource}'
            """

            ad_group_label_resources = []
            try:
                agl_response = ga_service.search(customer_id=customer_id, query=ad_group_labels_query)
                for row in agl_response:
                    ad_group_label_resources.append(row.ad_group_label.resource_name)
            except Exception as e:
                logger.error(f"Customer {customer_id}: Error querying ad group labels: {e}")
                continue

            if not ad_group_label_resources:
                logger.info(f"Customer {customer_id}: No ad groups with SD_CHECKED label")
                continue

            logger.info(f"Customer {customer_id}: Found {len(ad_group_label_resources)} ad groups with SD_CHECKED label")

            # Remove labels in batches
            batch_size = 5000
            for i in range(0, len(ad_group_label_resources), batch_size):
                batch = ad_group_label_resources[i:i + batch_size]

                operations = []
                for resource in batch:
                    operation = client.get_type("AdGroupLabelOperation")
                    operation.remove = resource
                    operations.append(operation)

                try:
                    response = ad_group_label_service.mutate_ad_group_labels(
                        customer_id=customer_id,
                        operations=operations
                    )
                    removed_count = len(response.results)
                    total_removed += removed_count
                    logger.info(f"Customer {customer_id}: Removed {removed_count} SD_CHECKED labels (batch {i//batch_size + 1})")
                except Exception as e:
                    logger.error(f"Customer {customer_id}: Error removing labels: {e}")

        except Exception as e:
            logger.error(f"Customer {customer_id}: Unexpected error: {e}", exc_info=True)

    logger.info(f"Total SD_CHECKED labels removed: {total_removed}")


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
        print(f"\nThis will remove SD_CHECKED labels from ALL ad groups across {len(customer_ids)} customers.")
        confirm = input("Are you sure you want to proceed? (yes/no): ")

        if confirm.lower() != 'yes':
            logger.info("Operation cancelled")
            sys.exit(0)

    # Run the removal
    asyncio.run(remove_sd_checked_labels(customer_ids))
