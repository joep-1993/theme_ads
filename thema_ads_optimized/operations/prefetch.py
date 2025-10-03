"""Bulk data prefetching for performance optimization."""

import asyncio
import logging
from typing import Dict, List
from google.ads.googleads.client import GoogleAdsClient
from models import CachedData, ExistingAd
from utils.retry import async_retry

logger = logging.getLogger(__name__)


@async_retry(max_attempts=3, delay=1.0)
async def prefetch_labels(client: GoogleAdsClient, customer_id: str) -> Dict[str, str]:
    """Fetch all labels for a customer in one query."""

    def _fetch():
        ga_service = client.get_service("GoogleAdsService")
        query = """
            SELECT label.resource_name, label.name
            FROM label
        """

        labels = {}
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                labels[row.label.name] = row.label.resource_name
            logger.debug(f"Prefetched {len(labels)} labels for customer {customer_id}")
        except Exception as e:
            logger.warning(f"Failed to prefetch labels for {customer_id}: {e}")

        return labels

    # Run in executor to avoid blocking
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch)


@async_retry(max_attempts=3, delay=1.0)
async def prefetch_existing_ads_bulk(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resources: List[str],
    batch_size: int = 7500
) -> Dict[str, ExistingAd]:
    """Fetch all existing RSAs for multiple ad groups in batched queries."""

    def _fetch():
        ga_service = client.get_service("GoogleAdsService")

        # Build resource list for query
        if not ad_group_resources:
            return {}

        # Use dynamic batch size
        ads_map = {}

        try:
            for i in range(0, len(ad_group_resources), batch_size):
                batch = ad_group_resources[i:i + batch_size]
                resources_str = ", ".join(f"'{r}'" for r in batch)

                query = f"""
                    SELECT
                        ad_group_ad.ad_group,
                        ad_group_ad.resource_name,
                        ad_group_ad.status,
                        ad_group_ad.ad.id,
                        ad_group_ad.ad.final_urls,
                        ad_group_ad.ad.responsive_search_ad.headlines,
                        ad_group_ad.ad.responsive_search_ad.descriptions,
                        ad_group_ad.ad.responsive_search_ad.path1,
                        ad_group_ad.ad.responsive_search_ad.path2
                    FROM ad_group_ad
                    WHERE ad_group_ad.ad_group IN ({resources_str})
                        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                        AND ad_group_ad.status != REMOVED
                    ORDER BY ad_group_ad.status ASC
                """

                response = ga_service.search(customer_id=customer_id, query=query)

                for row in response:
                    ad_group_resource = row.ad_group_ad.ad_group

                    # Only store first (best) ad per ad group
                    if ad_group_resource in ads_map:
                        continue

                    rsa = row.ad_group_ad.ad.responsive_search_ad
                    headlines = [a.text for a in getattr(rsa, "headlines", [])] if rsa and rsa.headlines else []
                    descriptions = [a.text for a in getattr(rsa, "descriptions", [])] if rsa and rsa.descriptions else []
                    final_urls = list(row.ad_group_ad.ad.final_urls) if row.ad_group_ad.ad.final_urls else []

                    ads_map[ad_group_resource] = ExistingAd(
                        resource_name=row.ad_group_ad.resource_name,
                        status=str(row.ad_group_ad.status),
                        headlines=headlines,
                        descriptions=descriptions,
                        final_urls=final_urls,
                        path1=getattr(rsa, "path1", "") or "",
                        path2=getattr(rsa, "path2", "") or ""
                    )

            logger.info(f"Prefetched {len(ads_map)} existing ads for {len(ad_group_resources)} ad groups (in {len(ad_group_resources)//batch_size + 1} batches)")
        except Exception as e:
            logger.error(f"Failed to prefetch ads for customer {customer_id}: {e}")

        return ads_map

    # Run in executor to avoid blocking
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch)


@async_retry(max_attempts=3, delay=1.0)
async def prefetch_ad_group_labels(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resources: List[str],
    label_name: str = "SD_DONE",
    batch_size: int = 7500
) -> Dict[str, bool]:
    """Check which ad groups have a specific label. Returns {ad_group_resource: has_label}."""

    def _fetch():
        if not ad_group_resources:
            return {}

        ga_service = client.get_service("GoogleAdsService")
        ag_labels_map = {ag_res: False for ag_res in ad_group_resources}

        # First get the label resource for SD_DONE
        label_query = f"""
            SELECT label.resource_name, label.name
            FROM label
            WHERE label.name = '{label_name}'
        """
        sd_done_resource = None
        try:
            label_search = ga_service.search(customer_id=customer_id, query=label_query)
            for row in label_search:
                sd_done_resource = row.label.resource_name
                break
        except Exception:
            pass

        if not sd_done_resource:
            logger.info(f"Label {label_name} doesn't exist yet, no ad groups to skip")
            return ag_labels_map

        # Use dynamic batch size
        try:
            for i in range(0, len(ad_group_resources), batch_size):
                batch = ad_group_resources[i:i + batch_size]
                resources_str = ", ".join(f"'{r}'" for r in batch)

                query = f"""
                    SELECT
                        ad_group_label.ad_group,
                        ad_group_label.label
                    FROM ad_group_label
                    WHERE ad_group_label.ad_group IN ({resources_str})
                """

                response = ga_service.search(customer_id=customer_id, query=query)

                for row in response:
                    if row.ad_group_label.label == sd_done_resource:
                        ag_labels_map[row.ad_group_label.ad_group] = True

            has_label_count = sum(1 for v in ag_labels_map.values() if v)
            logger.info(f"Found {has_label_count} ad groups with {label_name} label (checked in {len(ad_group_resources)//batch_size + 1} batches)")

        except Exception as e:
            logger.warning(f"Failed to check ad group labels: {e}")

        return ag_labels_map

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch)


async def prefetch_customer_data(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resources: List[str],
    batch_size: int = 7500
) -> CachedData:
    """Prefetch all required data for a customer in parallel."""

    logger.info(f"Prefetching data for customer {customer_id} ({len(ad_group_resources)} ad groups, batch_size={batch_size})")

    # Fetch labels, ads, and ad group labels in parallel
    labels_task = prefetch_labels(client, customer_id)
    ads_task = prefetch_existing_ads_bulk(client, customer_id, ad_group_resources, batch_size=batch_size)
    ag_labels_task = prefetch_ad_group_labels(client, customer_id, ad_group_resources, "SD_DONE", batch_size=batch_size)

    labels, existing_ads, ag_has_sd_done = await asyncio.gather(labels_task, ads_task, ag_labels_task)

    logger.info(
        f"Prefetch complete for {customer_id}: "
        f"{len(labels)} labels, {len(existing_ads)} ads"
    )

    return CachedData(
        labels=labels,
        existing_ads=existing_ads,
        campaigns={},  # Not needed for this use case
        ad_group_labels=ag_has_sd_done  # Add this field
    )
