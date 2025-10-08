"""
Optimized Thema Ads Script - High Performance Version

This script processes Google Ads themed campaigns with the following optimizations:
- Async/parallel processing (20-50x faster)
- Bulk API operations (10x fewer API calls)
- Smart prefetching and caching
- Batch mutations
- Retry logic with exponential backoff
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict
from collections import defaultdict
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import load_config_from_env
from google_ads_client import initialize_client
from models import AdGroupInput, ProcessingResult
from processors.data_loader import load_data
from operations.prefetch import prefetch_customer_data
from operations.labels import ensure_labels_exist, label_ads_batch, label_ad_groups_batch
from operations.ads import create_rsa_batch, build_ad_data
from templates.generators import generate_themed_content


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('thema_ads_optimized.log')
    ]
)
logger = logging.getLogger(__name__)


class ThemaAdsProcessor:
    """High-performance processor for themed ad campaigns."""

    def __init__(self, config, batch_size: int = 5000, skip_sd_done_check: bool = False):
        self.config = config
        self.client = initialize_client(config.google_ads)
        self.theme = "singles_day"  # Configurable
        self.label_names = ["SINGLES_DAY", "THEMA_AD", "THEMA_ORIGINAL", "BF_2025", "SD_DONE"]
        self.batch_size = batch_size
        self.skip_sd_done_check = skip_sd_done_check
        logger.info(f"Initialized ThemaAdsProcessor with batch_size={batch_size}, skip_sd_done_check={skip_sd_done_check}")

    async def process_all(self, inputs: List[AdGroupInput]) -> List[ProcessingResult]:
        """Process all ad groups with maximum parallelization."""

        logger.info(f"Starting processing of {len(inputs)} ad groups")
        start_time = time.time()

        # Group by customer_id for optimal batching
        by_customer = defaultdict(list)
        for inp in inputs:
            by_customer[inp.customer_id].append(inp)

        logger.info(f"Processing {len(by_customer)} customers")

        # Process customers in parallel with semaphore
        semaphore = asyncio.Semaphore(self.config.performance.max_concurrent_customers)

        async def process_with_limit(customer_id, customer_inputs):
            async with semaphore:
                result = await self.process_customer(customer_id, customer_inputs)
                # Add delay between customers to avoid rate limits
                await asyncio.sleep(self.config.performance.customer_delay)
                return result

        tasks = [
            process_with_limit(cid, inputs_list)
            for cid, inputs_list in by_customer.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results
        all_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Customer processing failed: {result}")
            else:
                all_results.extend(result)

        elapsed = time.time() - start_time
        success_count = sum(1 for r in all_results if r.success)

        logger.info(
            f"Processing complete: {success_count}/{len(all_results)} successful "
            f"in {elapsed:.2f}s ({len(all_results)/elapsed:.1f} ad groups/sec)"
        )

        return all_results

    async def _resolve_ad_group_ids(
        self,
        customer_id: str,
        inputs: List[AdGroupInput]
    ) -> List[AdGroupInput]:
        """Resolve ad_group_id from ad_group_name when name is provided.
        Excel scientific notation corrupts IDs, so we look up correct IDs by name.
        """
        # Separate inputs that need lookup vs those that don't
        inputs_needing_lookup = [inp for inp in inputs if inp.ad_group_name]
        inputs_ready = [inp for inp in inputs if not inp.ad_group_name]

        if not inputs_needing_lookup:
            return inputs  # No lookups needed

        def _lookup():
            """Lookup ad group IDs by name in a single batched query."""
            ga_service = self.client.get_service("GoogleAdsService")

            # Build name filter
            names = [inp.ad_group_name for inp in inputs_needing_lookup]
            names_str = ", ".join(f"'{name}'" for name in set(names))

            # Query ad groups by name
            query = f"""
                SELECT ad_group.id, ad_group.name
                FROM ad_group
                WHERE ad_group.name IN ({names_str})
            """

            try:
                response = ga_service.search(customer_id=customer_id, query=query)
                name_to_id = {row.ad_group.name: str(row.ad_group.id) for row in response}

                logger.info(f"Resolved {len(name_to_id)} ad group IDs from names for customer {customer_id}")

                # Update inputs with correct IDs
                corrected_inputs = []
                for inp in inputs_needing_lookup:
                    if inp.ad_group_name in name_to_id:
                        # Create new input with correct ID
                        corrected_inp = AdGroupInput(
                            customer_id=inp.customer_id,
                            campaign_name=inp.campaign_name,
                            campaign_id=inp.campaign_id,
                            ad_group_id=name_to_id[inp.ad_group_name],
                            ad_group_name=inp.ad_group_name
                        )
                        corrected_inputs.append(corrected_inp)
                    else:
                        logger.warning(f"Could not find ad group '{inp.ad_group_name}' for customer {customer_id}")
                        corrected_inputs.append(inp)  # Use original (will likely fail)

                return corrected_inputs

            except Exception as e:
                logger.error(f"Failed to resolve ad group names: {e}")
                return inputs_needing_lookup  # Return original inputs

        # Run lookup in executor
        loop = asyncio.get_event_loop()
        corrected_inputs = await loop.run_in_executor(None, _lookup)

        # Combine corrected and ready inputs
        return corrected_inputs + inputs_ready

    async def process_customer(
        self,
        customer_id: str,
        inputs: List[AdGroupInput]
    ) -> List[ProcessingResult]:
        """Process all ad groups for a single customer."""

        logger.info(f"Processing customer {customer_id}: {len(inputs)} ad groups")

        try:
            # Resolve ad group names to correct IDs (Excel scientific notation corrupts IDs)
            inputs_with_correct_ids = await self._resolve_ad_group_ids(customer_id, inputs)

            # Build ad group resource names
            ag_service = self.client.get_service("AdGroupService")
            ad_group_resources = [
                ag_service.ad_group_path(customer_id, inp.ad_group_id)
                for inp in inputs_with_correct_ids
            ]

            # Step 1: Prefetch all data (2-3 API calls)
            cached_data = await prefetch_customer_data(
                self.client,
                customer_id,
                ad_group_resources,
                batch_size=self.batch_size
            )

            # Step 2: Ensure all labels exist (1 API call)
            labels = await ensure_labels_exist(
                self.client,
                customer_id,
                self.label_names,
                cached_data.labels
            )

            # Step 3: Build operations in memory (no API calls)
            ad_operations = []
            label_operations_ads = []
            label_operations_ad_groups = []
            old_ads_to_label = []

            skipped_ags = []
            processed_inputs = []
            failed_inputs = []  # Track inputs that failed pre-checks

            for inp, ag_resource in zip(inputs, ad_group_resources):
                # Skip ad groups that already have SD_DONE label (unless this is a repair job)
                if not self.skip_sd_done_check and cached_data.ad_group_labels and cached_data.ad_group_labels.get(ag_resource, False):
                    logger.info(f"Skipping ad group {inp.ad_group_id} - already has SD_DONE label")
                    skipped_ags.append(inp)
                    continue

                result = self._build_operations_for_ad_group(
                    inp,
                    ag_resource,
                    cached_data,
                    labels
                )

                if result:
                    # Track inputs that successfully built operations
                    processed_inputs.append(inp)
                    ad_operations.append(result["ad_data"])
                    label_operations_ads.extend(result["ad_labels"])
                    label_operations_ad_groups.extend(result["ag_labels"])
                    if result["old_ad"]:
                        old_ads_to_label.append(result["old_ad"])
                else:
                    # Track inputs that failed pre-checks (no existing ad or no final URL)
                    failed_inputs.append(inp)

            if skipped_ags:
                logger.info(f"Skipped {len(skipped_ags)} ad groups that already have SD_DONE label")

            logger.info(
                f"Customer {customer_id}: Prepared {len(ad_operations)} ads, "
                f"{len(label_operations_ads)} ad labels, "
                f"{len(label_operations_ad_groups)} ad group labels"
            )

            # Step 4: Execute all mutations in batches (3-4 API calls total)
            if self.config.dry_run:
                logger.info(f"DRY RUN: Would create {len(ad_operations)} ads")
                return [
                    ProcessingResult(
                        customer_id=inp.customer_id,
                        ad_group_id=inp.ad_group_id,
                        success=True,
                        operations_count=1
                    )
                    for inp in inputs
                ]

            # Create new ads
            creation_result = await create_rsa_batch(
                self.client,
                customer_id,
                ad_operations
            )

            new_ad_resources = creation_result["resources"]
            creation_failures = creation_result["failures"]

            # Build ad_group_resource -> error map for failed creations
            failure_map = {}
            for failure in creation_failures:
                failure_map[failure["ad_group_resource"]] = failure["error"]

            # Label old ads
            if old_ads_to_label:
                await label_ads_batch(
                    self.client,
                    customer_id,
                    [(ad, labels["THEMA_ORIGINAL"]) for ad in old_ads_to_label]
                )

            # Label new ads
            if new_ad_resources:
                # Map label operations to new ad resources
                new_label_ops = []
                for ad_res in new_ad_resources:
                    # Each ad gets SINGLES_DAY label (THEMA_AD disabled to reduce API operations)
                    new_label_ops.append((ad_res, labels["SINGLES_DAY"]))
                    # new_label_ops.append((ad_res, labels["THEMA_AD"]))  # Disabled

                await label_ads_batch(self.client, customer_id, new_label_ops)

            # Label ad groups (only successful ones)
            if label_operations_ad_groups and new_ad_resources:
                await label_ad_groups_batch(
                    self.client,
                    customer_id,
                    label_operations_ad_groups
                )

            # Build results
            results = []

            # Add results for processed ad groups (match with ad_operations)
            for i, inp in enumerate(processed_inputs):
                ad_group_res = ad_operations[i]["ad_group_resource"]

                # Check if this ad group had a creation failure
                if ad_group_res in failure_map:
                    results.append(
                        ProcessingResult(
                            customer_id=customer_id,
                            ad_group_id=inp.ad_group_id,
                            success=False,
                            error=f"Ad creation failed: {failure_map[ad_group_res]}",
                            operations_count=0
                        )
                    )
                elif i < len(new_ad_resources):
                    # Successfully created
                    results.append(
                        ProcessingResult(
                            customer_id=customer_id,
                            ad_group_id=inp.ad_group_id,
                            success=True,
                            new_ad_resource=new_ad_resources[i],
                            operations_count=1
                        )
                    )
                else:
                    # Shouldn't happen, but handle gracefully
                    results.append(
                        ProcessingResult(
                            customer_id=customer_id,
                            ad_group_id=inp.ad_group_id,
                            success=False,
                            error="Ad creation failed (no resource returned, no error info)",
                            operations_count=0
                        )
                    )

            # Add results for skipped ad groups (mark as success since they were already processed)
            for inp in skipped_ags:
                results.append(
                    ProcessingResult(
                        customer_id=customer_id,
                        ad_group_id=inp.ad_group_id,
                        success=True,
                        new_ad_resource=None,
                        error="Already processed (has SD_DONE label)",
                        operations_count=0
                    )
                )

            # Add results for failed ad groups (no existing ad or no final URL)
            for inp in failed_inputs:
                results.append(
                    ProcessingResult(
                        customer_id=customer_id,
                        ad_group_id=inp.ad_group_id,
                        success=False,
                        error="No existing ad found or no final URL available",
                        operations_count=0
                    )
                )

            return results

        except Exception as e:
            logger.error(f"Failed to process customer {customer_id}: {e}", exc_info=True)
            return [
                ProcessingResult(
                    customer_id=customer_id,
                    ad_group_id=inp.ad_group_id,
                    success=False,
                    error=str(e)
                )
                for inp in inputs
            ]

    def _build_operations_for_ad_group(
        self,
        inp: AdGroupInput,
        ad_group_resource: str,
        cached_data,
        labels: Dict[str, str]
    ) -> dict:
        """Build all operations for a single ad group."""

        # Get existing ad from cache
        existing_ad = cached_data.existing_ads.get(ad_group_resource)

        if not existing_ad:
            logger.debug(f"No existing ad for ad group {inp.ad_group_id}")
            return None

        if not existing_ad.final_urls:
            logger.debug(f"No final URL for ad group {inp.ad_group_id}")
            return None

        final_url = existing_ad.final_urls[0]
        base_headlines_3 = existing_ad.headlines[:3]
        base_desc_1 = existing_ad.descriptions[0] if existing_ad.descriptions else ""

        # Generate themed content
        extra_headlines, extra_descriptions, path1 = generate_themed_content(
            self.theme,
            base_headlines_3,
            base_desc_1
        )

        # Build ad data
        ad_data = build_ad_data(
            ad_group_resource=ad_group_resource,
            final_url=final_url,
            base_headlines=base_headlines_3,
            base_description=base_desc_1,
            extra_headlines=extra_headlines,
            extra_descriptions=extra_descriptions,
            path1=path1,
            path2=existing_ad.path2 or existing_ad.path1 or ""
        )

        # Build label operations
        ad_labels = []  # Will be filled after ad creation
        ag_labels = [
            # (ad_group_resource, labels["BF_2025"]),  # Disabled to reduce API operations
            (ad_group_resource, labels["SD_DONE"])
        ]

        return {
            "ad_data": ad_data,
            "ad_labels": ad_labels,
            "ag_labels": ag_labels,
            "old_ad": existing_ad.resource_name
        }


async def main():
    """Main entry point."""

    try:
        # Load configuration
        config = load_config_from_env()
        logger.info("Configuration loaded successfully")

        # Load input data
        inputs = load_data(config.input_file)
        logger.info(f"Loaded {len(inputs)} ad groups from {config.input_file}")

        if not inputs:
            logger.warning("No ad groups to process")
            return

        # Process
        processor = ThemaAdsProcessor(config)
        results = await processor.process_all(inputs)

        # Summary
        success_count = sum(1 for r in results if r.success)
        failed_count = len(results) - success_count

        logger.info("=" * 60)
        logger.info(f"SUMMARY: {success_count} successful, {failed_count} failed")
        logger.info("=" * 60)

        if failed_count > 0:
            logger.warning("Failed ad groups:")
            for r in results:
                if not r.success:
                    logger.warning(f"  - {r.customer_id} / {r.ad_group_id}: {r.error}")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
