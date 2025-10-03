"""Ad operations for creating and managing RSAs."""

import asyncio
import logging
from typing import List
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from utils.retry import async_retry

logger = logging.getLogger(__name__)


@async_retry(max_attempts=3, delay=1.0)
async def create_rsa_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_data_list: List[dict]  # List of ad configurations
) -> List[str]:
    """Create multiple RSAs in batch. Returns list of resource names.

    Google Ads API limits to 10,000 operations per request.
    This function automatically chunks larger batches.
    """

    def _create():
        if not ad_data_list:
            return []

        service = client.get_service("AdGroupAdService")
        all_resource_names = []

        # Google Ads API limit: 10,000 operations per request
        BATCH_LIMIT = 10000

        # Process in chunks
        for chunk_start in range(0, len(ad_data_list), BATCH_LIMIT):
            chunk = ad_data_list[chunk_start:chunk_start + BATCH_LIMIT]
            operations = []

            for ad_data in chunk:
                op = client.get_type("AdGroupAdOperation")
                aga = op.create

                aga.ad_group = ad_data["ad_group_resource"]
                aga.status = client.enums.AdGroupAdStatusEnum.PAUSED

                ad = aga.ad
                ad.final_urls.append(ad_data["final_url"])

                rsa = ad.responsive_search_ad

                # Add headlines
                for headline in ad_data.get("headlines", [])[:15]:
                    if headline:
                        asset = client.get_type("AdTextAsset")
                        asset.text = headline
                        rsa.headlines.append(asset)

                # Add descriptions
                for description in ad_data.get("descriptions", [])[:4]:
                    if description:
                        asset = client.get_type("AdTextAsset")
                        asset.text = description
                        rsa.descriptions.append(asset)

                # Add paths
                if ad_data.get("path1"):
                    rsa.path1 = ad_data["path1"]
                if ad_data.get("path2"):
                    rsa.path2 = ad_data["path2"]

                operations.append(op)

            try:
                response = service.mutate_ad_group_ads(
                    customer_id=customer_id,
                    operations=operations
                )

                resource_names = [res.resource_name for res in response.results]
                all_resource_names.extend(resource_names)
                logger.info(f"Created {len(resource_names)} RSAs in chunk (batch {chunk_start//BATCH_LIMIT + 1}/{(len(ad_data_list)-1)//BATCH_LIMIT + 1})")

            except GoogleAdsException as e:
                logger.error(f"Failed to create RSAs in chunk {chunk_start//BATCH_LIMIT + 1}: {e}")
                # Continue with next chunk instead of failing completely

        logger.info(f"Created {len(all_resource_names)} RSAs total across {(len(ad_data_list)-1)//BATCH_LIMIT + 1} chunks")
        return all_resource_names

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _create)


@async_retry(max_attempts=3, delay=1.0)
async def pause_ads_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_resource_names: List[str]
) -> int:
    """Pause multiple ads in batch. Returns count of successfully paused ads."""

    def _pause():
        if not ad_resource_names:
            return 0

        service = client.get_service("AdGroupAdService")
        operations = []

        for ad_resource in ad_resource_names:
            op = client.get_type("AdGroupAdOperation")
            op.update.resource_name = ad_resource
            op.update.status = client.enums.AdGroupAdStatusEnum.PAUSED
            op.update_mask.paths.append("status")
            operations.append(op)

        try:
            response = service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=operations
            )
            logger.debug(f"Paused {len(response.results)} ads")
            return len(response.results)

        except GoogleAdsException as e:
            logger.warning(f"Some ads failed to pause: {e}")
            return 0

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _pause)


def build_ad_data(
    ad_group_resource: str,
    final_url: str,
    base_headlines: List[str],
    base_description: str,
    extra_headlines: List[str],
    extra_descriptions: List[str],
    path1: str,
    path2: str
) -> dict:
    """Build ad data structure for batch creation."""

    all_headlines = (base_headlines or []) + (extra_headlines or [])
    all_headlines = [h for h in all_headlines if h][:15]

    all_descriptions = ([base_description] if base_description else []) + (extra_descriptions or [])
    all_descriptions = [d for d in all_descriptions if d][:4]

    return {
        "ad_group_resource": ad_group_resource,
        "final_url": final_url,
        "headlines": all_headlines,
        "descriptions": all_descriptions,
        "path1": path1,
        "path2": path2
    }
