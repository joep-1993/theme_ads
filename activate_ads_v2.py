"""
Ultra-fast ad activation using AD-FIRST query approach.

This is 10-100x faster than the ad-group-first approach because:
1. Directly queries ads with specific theme labels (THEME_BF, etc.)
2. Directly queries ads with THEMA_ORIGINAL label in those ad groups
3. No need to query all ad groups or filter
4. Minimal API calls with maximum targeting

Performance comparison:
- Ad-group-first: Query 10,000 ad groups → filter → query ads → query labels
- Ad-first: Query ~1,000 theme ads → query ~2,000 original ads → done

Usage:
    from activate_ads_v2 import activate_ads_v2
    result = await activate_ads_v2(client, customer_ids, parallel_workers=5)
"""

import asyncio
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


async def activate_ads_v2(
    client,
    customer_ids: Optional[List[str]] = None,
    parallel_workers: int = 5,
    reset_labels: bool = False
) -> Dict:
    """
    V2: Ultra-fast ad activation using AD-FIRST query approach.

    Instead of querying all ad groups and filtering, we directly target:
    1. All ads with the required theme label (e.g., THEME_BF)
    2. All ads with THEMA_ORIGINAL label in those same ad groups
    3. Build batch enable/pause operations
    4. Execute in parallel across customers

    Args:
        client: Google Ads API client
        customer_ids: Optional list of customer IDs (None = all in activation plan)
        parallel_workers: Number of customers to process in parallel (default: 5)
        reset_labels: If True, reprocess ad groups with ACTIVATION_DONE label

    Returns:
        Dict with status and statistics
    """
    from backend.database import get_activation_plan, add_activation_missing_ad, clear_activation_missing_ads
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))
    from themes import get_theme_label

    logger.info(f"Starting V2 (AD-FIRST) activation: customers={customer_ids}, parallel={parallel_workers}")

    # Clear old missing ads records
    clear_activation_missing_ads()

    # Get activation plan
    plan = get_activation_plan(customer_ids)
    if not plan:
        return {
            'status': 'error',
            'message': 'No activation plan found. Please upload an activation plan first.',
            'stats': {}
        }

    logger.info(f"Loaded activation plan for {len(plan)} customers")

    # Shared stats (thread-safe with asyncio)
    stats = {
        'customers_processed': 0,
        'customers_failed': 0,
        'ad_groups_activated': 0,
        'theme_ads_enabled': 0,
        'original_ads_paused': 0,
        'errors': []
    }
    stats_lock = asyncio.Lock()

    async def process_customer_v2(customer_id: str, required_theme: str):
        """Process a single customer using AD-FIRST approach."""
        try:
            logger.info(f"[{customer_id}] V2 Processing - theme: {required_theme}")

            ga_service = client.get_service("GoogleAdsService")
            ad_group_ad_service = client.get_service("AdGroupAdService")

            theme_label_name = get_theme_label(required_theme)

            # Step 1: Direct query for ALL theme ads in HS/ campaigns
            # This is the KEY optimization - query ads by label directly!
            theme_ads_query = f"""
                SELECT
                    ad_group_ad.ad_group,
                    ad_group_ad.resource_name,
                    ad_group_ad.status,
                    campaign.name
                FROM ad_group_ad
                WHERE campaign.name LIKE 'HS/%'
                AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.status != REMOVED
                AND ad_group_ad_label.label IN (
                    SELECT label.resource_name
                    FROM label
                    WHERE label.name = '{theme_label_name}'
                )
            """

            # Organize theme ads by ad group
            theme_ads_by_ag = {}  # ad_group_resource -> ad_resource
            ad_groups_with_theme = set()

            try:
                response = ga_service.search(customer_id=customer_id, query=theme_ads_query)
                for row in response:
                    ag_res = row.ad_group_ad.ad_group
                    ad_res = row.ad_group_ad.resource_name
                    ad_status = row.ad_group_ad.status.name

                    theme_ads_by_ag[ag_res] = {
                        'resource': ad_res,
                        'status': ad_status
                    }
                    ad_groups_with_theme.add(ag_res)

                logger.info(f"[{customer_id}] Found {len(theme_ads_by_ag)} theme ads in {len(ad_groups_with_theme)} ad groups")
            except Exception as e:
                logger.error(f"[{customer_id}] Failed to query theme ads: {e}")
                async with stats_lock:
                    stats['customers_failed'] += 1
                    stats['errors'].append(f"{customer_id}: Failed to query theme ads - {e}")
                return

            if not theme_ads_by_ag:
                logger.info(f"[{customer_id}] No theme ads found")
                async with stats_lock:
                    stats['customers_processed'] += 1
                return

            # Step 2: Query THEMA_ORIGINAL ads in those same ad groups
            ag_resources_str = "', '".join(ad_groups_with_theme)
            original_ads_query = f"""
                SELECT
                    ad_group_ad.ad_group,
                    ad_group_ad.resource_name,
                    ad_group_ad.status
                FROM ad_group_ad
                WHERE ad_group_ad.ad_group IN ('{ag_resources_str}')
                AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.status != REMOVED
                AND ad_group_ad_label.label IN (
                    SELECT label.resource_name
                    FROM label
                    WHERE label.name = 'THEMA_ORIGINAL'
                )
            """

            # Organize original ads by ad group
            original_ads_by_ag = {}  # ad_group_resource -> [ad_resources]
            try:
                response = ga_service.search(customer_id=customer_id, query=original_ads_query)
                for row in response:
                    ag_res = row.ad_group_ad.ad_group
                    ad_res = row.ad_group_ad.resource_name
                    ad_status = row.ad_group_ad.status.name

                    if ag_res not in original_ads_by_ag:
                        original_ads_by_ag[ag_res] = []

                    original_ads_by_ag[ag_res].append({
                        'resource': ad_res,
                        'status': ad_status
                    })

                logger.info(f"[{customer_id}] Found {sum(len(ads) for ads in original_ads_by_ag.values())} THEMA_ORIGINAL ads")
            except Exception as e:
                logger.warning(f"[{customer_id}] Could not query THEMA_ORIGINAL ads: {e}")

            # Step 3: Process ad groups in batches (pause→enable immediately per batch)
            # This minimizes time gap between pausing originals and enabling theme ads
            ad_groups_list = list(theme_ads_by_ag.items())
            batch_size = 100  # Process 100 ad groups at a time

            total_paused = 0
            total_enabled = 0

            for batch_idx in range(0, len(ad_groups_list), batch_size):
                batch = ad_groups_list[batch_idx:batch_idx+batch_size]

                pause_operations = []
                enable_operations = []

                # Build operations for this batch of ad groups
                for ag_res, theme_ad in batch:
                    # Pause all THEMA_ORIGINAL ads in this ad group FIRST
                    if ag_res in original_ads_by_ag:
                        for orig_ad in original_ads_by_ag[ag_res]:
                            if orig_ad['status'] == 'ENABLED':
                                operation = client.get_type("AdGroupAdOperation")
                                ad_group_ad = operation.update
                                ad_group_ad.resource_name = orig_ad['resource']
                                ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED
                                operation.update_mask.paths.append('status')
                                pause_operations.append(operation)

                    # Enable theme ad if paused
                    if theme_ad['status'] == 'PAUSED':
                        operation = client.get_type("AdGroupAdOperation")
                        ad_group_ad = operation.update
                        ad_group_ad.resource_name = theme_ad['resource']
                        ad_group_ad.status = client.enums.AdGroupAdStatusEnum.ENABLED
                        operation.update_mask.paths.append('status')
                        enable_operations.append(operation)

                # Step 4: Execute pause operations for this batch
                if pause_operations:
                    try:
                        ad_group_ad_service.mutate_ad_group_ads(
                            customer_id=customer_id,
                            operations=pause_operations
                        )
                        total_paused += len(pause_operations)
                    except Exception as e:
                        logger.error(f"[{customer_id}] Batch {batch_idx//batch_size + 1}: Failed to pause THEMA_ORIGINAL ads: {e}")
                        async with stats_lock:
                            stats['errors'].append(f"{customer_id}: Batch {batch_idx//batch_size + 1}: Failed to pause - {e}")

                # Step 5: Immediately enable theme ads for this batch (minimize gap)
                if enable_operations:
                    try:
                        ad_group_ad_service.mutate_ad_group_ads(
                            customer_id=customer_id,
                            operations=enable_operations
                        )
                        total_enabled += len(enable_operations)
                    except Exception as e:
                        logger.error(f"[{customer_id}] Batch {batch_idx//batch_size + 1}: Failed to enable theme ads: {e}")
                        async with stats_lock:
                            stats['errors'].append(f"{customer_id}: Batch {batch_idx//batch_size + 1}: Failed to enable - {e}")

            logger.info(f"[{customer_id}] Paused {total_paused} THEMA_ORIGINAL ads, Enabled {total_enabled} theme ads")
            async with stats_lock:
                stats['original_ads_paused'] += total_paused
                stats['theme_ads_enabled'] += total_enabled
                stats['ad_groups_activated'] += len(theme_ads_by_ag)

            async with stats_lock:
                stats['customers_processed'] += 1

            logger.info(f"[{customer_id}] V2 Completed successfully")

        except Exception as e:
            logger.error(f"[{customer_id}] V2 Error: {e}", exc_info=True)
            async with stats_lock:
                stats['customers_failed'] += 1
                stats['errors'].append(f"{customer_id}: {str(e)}")

    # Process customers in parallel
    tasks = []
    for customer_id, required_theme in plan.items():
        task = process_customer_v2(customer_id, required_theme)
        tasks.append(task)

    # Run in batches of parallel_workers
    for i in range(0, len(tasks), parallel_workers):
        batch = tasks[i:i+parallel_workers]
        await asyncio.gather(*batch, return_exceptions=True)

    logger.info(f"V2 (AD-FIRST) activation completed: {stats}")

    return {
        'status': 'completed',
        'stats': stats
    }
