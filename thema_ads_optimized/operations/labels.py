"""Label operations for campaigns, ad groups, and ads."""

import asyncio
import logging
from typing import List, Dict
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from utils.retry import async_retry

logger = logging.getLogger(__name__)


@async_retry(max_attempts=3, delay=1.0)
async def ensure_labels_exist(
    client: GoogleAdsClient,
    customer_id: str,
    label_names: List[str],
    existing_labels: Dict[str, str]
) -> Dict[str, str]:
    """Ensure labels exist, create missing ones. Returns {label_name: resource_name}."""

    def _ensure():
        # Check which labels need to be created
        needed = [name for name in label_names if name not in existing_labels]

        if not needed:
            return existing_labels

        # Batch create missing labels
        label_service = client.get_service("LabelService")
        operations = []

        for label_name in needed:
            op = client.get_type("LabelOperation")
            op.create.name = label_name
            operations.append(op)

        try:
            response = label_service.mutate_labels(
                customer_id=customer_id,
                operations=operations
            )

            # Update the map
            result = existing_labels.copy()
            for i, res in enumerate(response.results):
                result[needed[i]] = res.resource_name

            logger.info(f"Created {len(needed)} new labels for customer {customer_id}")
            return result

        except GoogleAdsException as e:
            logger.error(f"Failed to create labels: {e}")
            # Return what we have
            return existing_labels

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ensure)


@async_retry(max_attempts=3, delay=1.0)
async def label_ads_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_label_pairs: List[tuple]  # [(ad_group_ad_resource, label_resource), ...]
) -> int:
    """Label multiple ads in batch. Returns count of successful labels.

    Google Ads API limits to 10,000 operations per request.
    This function automatically chunks larger batches.
    """

    def _label():
        if not ad_label_pairs:
            return 0

        service = client.get_service("AdGroupAdLabelService")
        total_labeled = 0

        # Google Ads API limit: 10,000 operations per request
        BATCH_LIMIT = 10000

        # Process in chunks
        for chunk_start in range(0, len(ad_label_pairs), BATCH_LIMIT):
            chunk = ad_label_pairs[chunk_start:chunk_start + BATCH_LIMIT]
            operations = []

            for ad_resource, label_resource in chunk:
                op = client.get_type("AdGroupAdLabelOperation")
                op.create.ad_group_ad = ad_resource
                op.create.label = label_resource
                operations.append(op)

            try:
                response = service.mutate_ad_group_ad_labels(
                    customer_id=customer_id,
                    operations=operations
                )
                total_labeled += len(response.results)
                logger.debug(f"Labeled {len(response.results)} ads in chunk {chunk_start//BATCH_LIMIT + 1}")

            except GoogleAdsException as e:
                logger.warning(f"Some ad labels failed in chunk {chunk_start//BATCH_LIMIT + 1}: {e}")

        logger.info(f"Labeled {total_labeled} ads total")
        return total_labeled

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _label)


@async_retry(max_attempts=3, delay=1.0)
async def label_ad_groups_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_label_pairs: List[tuple]  # [(ad_group_resource, label_resource), ...]
) -> int:
    """Label multiple ad groups in batch. Returns count of successful labels.

    Google Ads API limits to 10,000 operations per request.
    This function automatically chunks larger batches.
    """

    def _label():
        if not ad_group_label_pairs:
            return 0

        service = client.get_service("AdGroupLabelService")
        total_labeled = 0

        # Google Ads API limit: 10,000 operations per request
        BATCH_LIMIT = 10000

        # Process in chunks
        for chunk_start in range(0, len(ad_group_label_pairs), BATCH_LIMIT):
            chunk = ad_group_label_pairs[chunk_start:chunk_start + BATCH_LIMIT]
            operations = []

            for ag_resource, label_resource in chunk:
                op = client.get_type("AdGroupLabelOperation")
                op.create.ad_group = ag_resource
                op.create.label = label_resource
                operations.append(op)

            try:
                response = service.mutate_ad_group_labels(
                    customer_id=customer_id,
                    operations=operations
                )
                total_labeled += len(response.results)
                logger.debug(f"Labeled {len(response.results)} ad groups in chunk {chunk_start//BATCH_LIMIT + 1}")

            except GoogleAdsException as e:
                logger.warning(f"Some ad group labels failed in chunk {chunk_start//BATCH_LIMIT + 1}: {e}")

        logger.info(f"Labeled {total_labeled} ad groups total")
        return total_labeled

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _label)
