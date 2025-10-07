"""
Thema Ads Service - Integration with FastAPI and state persistence
"""
import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from backend.database import get_db_connection

# Configure logging
logger = logging.getLogger(__name__)

# Add thema_ads_optimized to path
THEMA_ADS_PATH = Path(__file__).parent.parent / "thema_ads_optimized"
sys.path.insert(0, str(THEMA_ADS_PATH))


class ThemaAdsService:
    """Service for managing Thema Ads processing with state persistence."""

    def __init__(self):
        self.current_job_id = None
        self.is_running = False

    def _fetch_campaign_info_with_client(self, client, customer_id: str, ad_group_id: str) -> Dict:
        """Fetch campaign information from Google Ads API using existing client."""
        try:
            # Query ad group to get campaign info
            ga_service = client.get_service("GoogleAdsService")
            query = f"""
                SELECT
                    ad_group.id,
                    ad_group.name,
                    campaign.id,
                    campaign.name
                FROM ad_group
                WHERE ad_group.id = {ad_group_id}
                LIMIT 1
            """

            response = ga_service.search(customer_id=customer_id, query=query)

            for row in response:
                return {
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name
                }

            raise ValueError(f"Ad group {ad_group_id} not found for customer {customer_id}")

        except Exception as e:
            logger.error(f"Failed to fetch campaign info: {e}")
            raise

    def create_job(self, input_data: List[Dict], batch_size: int = 7500) -> int:
        """Create a new processing job and store input data using batch inserts."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Create job with batch_size
            cur.execute("""
                INSERT INTO thema_ads_jobs (status, total_ad_groups, batch_size)
                VALUES ('pending', %s, %s)
                RETURNING id
            """, (len(input_data), batch_size))

            job_id = cur.fetchone()['id']

            # Batch insert input data (much faster than individual inserts)
            if input_data:
                input_values = [
                    (job_id, item['customer_id'], item.get('campaign_id'),
                     item.get('campaign_name'), item['ad_group_id'], item.get('ad_group_name'))
                    for item in input_data
                ]

                cur.executemany("""
                    INSERT INTO thema_ads_input_data (job_id, customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, input_values)

                # Batch insert job items
                job_item_values = [
                    (job_id, item['customer_id'], item.get('campaign_id'),
                     item.get('campaign_name'), item['ad_group_id'], item.get('ad_group_name'), 'pending')
                    for item in input_data
                ]

                cur.executemany("""
                    INSERT INTO thema_ads_job_items (job_id, customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, job_item_values)

            conn.commit()
            logger.info(f"Created job {job_id} with {len(input_data)} ad groups using batch inserts")
            return job_id

        finally:
            cur.close()
            conn.close()

    def get_job_status(self, job_id: int) -> Dict:
        """Get current status of a job."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Get job info
            cur.execute("""
                SELECT * FROM thema_ads_jobs WHERE id = %s
            """, (job_id,))

            job = cur.fetchone()
            if not job:
                return None

            # Get item statistics
            cur.execute("""
                SELECT
                    status,
                    COUNT(*) as count
                FROM thema_ads_job_items
                WHERE job_id = %s
                GROUP BY status
            """, (job_id,))

            items_by_status = {row['status']: row['count'] for row in cur.fetchall()}

            # Get recent failures
            cur.execute("""
                SELECT customer_id, ad_group_id, error_message
                FROM thema_ads_job_items
                WHERE job_id = %s AND status = 'failed'
                ORDER BY processed_at DESC
                LIMIT 10
            """, (job_id,))

            recent_failures = cur.fetchall()

            # Map database columns to API field names
            job_dict = dict(job)
            return {
                'id': job_dict['id'],
                'status': job_dict['status'],
                'total_items': job_dict.get('total_ad_groups', 0),
                'successful_items': items_by_status.get('successful', 0),
                'failed_items': items_by_status.get('failed', 0),
                'skipped_items': items_by_status.get('skipped', 0),
                'pending_items': items_by_status.get('pending', 0),
                'started_at': job_dict.get('started_at'),
                'completed_at': job_dict.get('completed_at'),
                'created_at': job_dict.get('created_at'),
                'updated_at': job_dict.get('updated_at'),
                'error_message': job_dict.get('error_message'),
                'batch_size': job_dict.get('batch_size', 7500),
                'items_by_status': items_by_status,
                'recent_failures': recent_failures
            }

        finally:
            cur.close()
            conn.close()

    def get_pending_items(self, job_id: int) -> List[Dict]:
        """Get all pending items for a job (for resume)."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name
                FROM thema_ads_job_items
                WHERE job_id = %s AND status = 'pending'
            """, (job_id,))

            return cur.fetchall()

        finally:
            cur.close()
            conn.close()

    def update_job_status(self, job_id: int, status: str, **kwargs):
        """Update job status."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            updates = ['status = %s', 'updated_at = CURRENT_TIMESTAMP']
            values = [status]

            if 'error_message' in kwargs:
                updates.append('error_message = %s')
                values.append(kwargs['error_message'])

            if status == 'running' and 'started_at' not in kwargs:
                updates.append('started_at = CURRENT_TIMESTAMP')

            if status in ('completed', 'failed'):
                updates.append('completed_at = CURRENT_TIMESTAMP')

            values.append(job_id)

            cur.execute(f"""
                UPDATE thema_ads_jobs
                SET {', '.join(updates)}
                WHERE id = %s
            """, values)

            conn.commit()

        finally:
            cur.close()
            conn.close()

    def update_item_status(self, job_id: int, customer_id: str, ad_group_id: str,
                          status: str, new_ad_resource: Optional[str] = None,
                          error_message: Optional[str] = None):
        """Update individual item status."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                UPDATE thema_ads_job_items
                SET status = %s,
                    new_ad_resource = %s,
                    error_message = %s,
                    processed_at = CURRENT_TIMESTAMP
                WHERE job_id = %s AND customer_id = %s AND ad_group_id = %s
            """, (status, new_ad_resource, error_message, job_id, customer_id, ad_group_id))

            # Update job statistics
            cur.execute("""
                UPDATE thema_ads_jobs
                SET processed_ad_groups = (
                        SELECT COUNT(*) FROM thema_ads_job_items
                        WHERE job_id = %s AND status IN ('successful', 'failed', 'skipped')
                    ),
                    successful_ad_groups = (
                        SELECT COUNT(*) FROM thema_ads_job_items
                        WHERE job_id = %s AND status = 'successful'
                    ),
                    failed_ad_groups = (
                        SELECT COUNT(*) FROM thema_ads_job_items
                        WHERE job_id = %s AND status = 'failed'
                    ),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (job_id, job_id, job_id, job_id))

            conn.commit()

        finally:
            cur.close()
            conn.close()

    async def process_job(self, job_id: int):
        """Process a job with state persistence."""
        try:
            import os
            from dotenv import load_dotenv

            # Load .env file from thema_ads_optimized directory
            env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
            if env_path.exists():
                logger.info(f"Loading environment from: {env_path}")
                load_dotenv(env_path)
            else:
                logger.warning(f"Environment file not found at: {env_path}")

            from config import load_config_from_env
            from google_ads_client import initialize_client
            from models import AdGroupInput

            # Load config
            config = load_config_from_env()

            # Get job details including batch_size
            job_details = self.get_job_status(job_id)
            batch_size = job_details.get('batch_size', 7500)
            logger.info(f"Job {job_id} will use batch_size: {batch_size}")

            # Get pending items
            pending_items = self.get_pending_items(job_id)

            if not pending_items:
                logger.info(f"No pending items for job {job_id}")
                self.update_job_status(job_id, 'completed')
                return

            # Initialize client for potential campaign info fetching
            client = initialize_client(config.google_ads)

            # Convert to AdGroupInput objects, fetching campaign info if missing
            inputs = []
            for item in pending_items:
                campaign_id = item['campaign_id']
                campaign_name = item['campaign_name']

                # Fetch campaign info if not in database
                if not campaign_id or not campaign_name:
                    logger.info(f"Fetching campaign info for ad group {item['ad_group_id']}")
                    campaign_info = self._fetch_campaign_info_with_client(
                        client,
                        item['customer_id'],
                        item['ad_group_id']
                    )
                    campaign_id = campaign_info['campaign_id']
                    campaign_name = campaign_info['campaign_name']

                inputs.append(AdGroupInput(
                    customer_id=item['customer_id'],
                    campaign_name=campaign_name,
                    campaign_id=campaign_id,
                    ad_group_id=item['ad_group_id'],
                    ad_group_name=item.get('ad_group_name')
                ))

            # Update job status
            self.update_job_status(job_id, 'running')
            self.current_job_id = job_id
            self.is_running = True

            logger.info(f"Starting job {job_id} with {len(inputs)} items, batch_size={batch_size}")

            # Import and initialize processor
            from main_optimized import ThemaAdsProcessor
            processor = ThemaAdsProcessor(config, batch_size=batch_size)

            # Process with custom callback
            results = await self._process_with_tracking(processor, inputs, job_id)

            # Update final status
            job_status = self.get_job_status(job_id)
            if job_status['failed_items'] == 0:
                self.update_job_status(job_id, 'completed')
            else:
                self.update_job_status(job_id, 'completed')

            self.is_running = False
            self.current_job_id = None

            logger.info(f"Job {job_id} completed")

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            self.update_job_status(job_id, 'failed', error_message=str(e))
            self.is_running = False
            self.current_job_id = None

    async def _process_with_tracking(self, processor, inputs, job_id):
        """Process inputs with progress tracking."""
        from collections import defaultdict

        # Group by customer
        by_customer = defaultdict(list)
        for inp in inputs:
            by_customer[inp.customer_id].append(inp)

        # Process customers
        semaphore = asyncio.Semaphore(processor.config.performance.max_concurrent_customers)

        async def process_with_tracking(customer_id, customer_inputs):
            async with semaphore:
                results = await processor.process_customer(customer_id, customer_inputs)

                # Update database with results
                for result, inp in zip(results, customer_inputs):
                    # Determine status based on result
                    if result.success and result.error and "Already processed" in result.error:
                        # Ad group already has SD_DONE label
                        status = 'skipped'
                    elif not result.success and result.error and "No existing ad" in result.error:
                        # Ad group has no existing ads to work with (not a failure, just can't process)
                        status = 'skipped'
                    elif result.success:
                        status = 'successful'
                    else:
                        status = 'failed'

                    self.update_item_status(
                        job_id,
                        customer_id,
                        inp.ad_group_id,
                        status,
                        result.new_ad_resource if result.success else None,
                        result.error
                    )

                return results

        tasks = [
            process_with_tracking(cid, inputs_list)
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

        return all_results

    def pause_job(self, job_id: int):
        """Pause a running job."""
        self.is_running = False
        self.update_job_status(job_id, 'paused')
        logger.info(f"Job {job_id} paused")

    def resume_job(self, job_id: int):
        """Resume a paused job."""
        asyncio.create_task(self.process_job(job_id))
        logger.info(f"Job {job_id} resumed")

    def list_jobs(self, limit: int = 20) -> List[Dict]:
        """List all jobs."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT
                    j.*,
                    COALESCE(SUM(CASE WHEN i.status = 'successful' THEN 1 ELSE 0 END), 0) as successful_count,
                    COALESCE(SUM(CASE WHEN i.status = 'failed' THEN 1 ELSE 0 END), 0) as failed_count,
                    COALESCE(SUM(CASE WHEN i.status = 'skipped' THEN 1 ELSE 0 END), 0) as skipped_count,
                    COALESCE(SUM(CASE WHEN i.status = 'pending' THEN 1 ELSE 0 END), 0) as pending_count
                FROM thema_ads_jobs j
                LEFT JOIN thema_ads_job_items i ON j.id = i.job_id
                GROUP BY j.id
                ORDER BY j.created_at DESC
                LIMIT %s
            """, (limit,))

            jobs = cur.fetchall()

            # Map database columns to API field names
            return [{
                'id': job['id'],
                'status': job['status'],
                'total_items': job.get('total_ad_groups', 0),
                'successful_items': job.get('successful_count', 0),
                'failed_items': job.get('failed_count', 0),
                'skipped_items': job.get('skipped_count', 0),
                'pending_items': job.get('pending_count', 0),
                'started_at': job.get('started_at'),
                'completed_at': job.get('completed_at'),
                'created_at': job.get('created_at'),
                'batch_size': job.get('batch_size', 7500)
            } for job in jobs]

        finally:
            cur.close()
            conn.close()

    def delete_job(self, job_id: int):
        """Delete a job and all associated data."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Delete job (will cascade to job_items and input_data)
            cur.execute("""
                DELETE FROM thema_ads_jobs
                WHERE id = %s
            """, (job_id,))

            conn.commit()
            logger.info(f"Deleted job {job_id}")

        finally:
            cur.close()
            conn.close()

    async def checkup_ad_groups(
        self,
        client,
        customer_ids: List[str],
        limit: Optional[int] = None,
        batch_size: int = 5000,
        job_chunk_size: int = 50000,
        background_tasks=None
    ) -> Dict:
        """
        Check ad groups with SD_DONE label to verify SINGLES_DAY ads exist.
        Creates repair jobs for ad groups missing SINGLES_DAY ads.

        Args:
            client: Google Ads API client
            customer_ids: List of customer IDs to check
            limit: Optional limit on number of ad groups to check
            batch_size: Batch size for API queries
            job_chunk_size: Maximum items per repair job
            background_tasks: FastAPI background tasks

        Returns:
            Dict with checkup results and created job IDs
        """
        logger.info(f"Starting checkup: limit={limit}, batch_size={batch_size}")

        ga_service = client.get_service("GoogleAdsService")
        label_service = client.get_service("LabelService")

        stats = {
            'customers_processed': 0,
            'ad_groups_checked': 0,
            'ad_groups_verified': 0,
            'ad_groups_missing_singles_day': 0,
            'sd_checked_labels_applied': 0,
            'repair_jobs_created': 0
        }

        repair_items = []  # Ad groups needing repair

        # Process each customer
        for customer_id in customer_ids:
            # Check if we've reached the limit
            if limit and stats['ad_groups_checked'] >= limit:
                logger.info(f"Reached limit of {limit} ad groups checked")
                break

            try:
                logger.info(f"Processing customer {customer_id}")
                stats['customers_processed'] += 1

                # Get SD_DONE label resource
                sd_done_query = """
                    SELECT label.resource_name, label.id
                    FROM label
                    WHERE label.name = 'SD_DONE'
                    LIMIT 1
                """
                sd_done_resource = None
                try:
                    sd_label_response = ga_service.search(customer_id=customer_id, query=sd_done_query)
                    for row in sd_label_response:
                        sd_done_resource = row.label.resource_name
                        break
                except Exception as e:
                    logger.warning(f"Customer {customer_id}: Could not find SD_DONE label: {e}")
                    continue

                if not sd_done_resource:
                    logger.info(f"Customer {customer_id}: No SD_DONE label found, skipping")
                    continue

                # Get SD_CHECKED label resource (or create it)
                sd_checked_query = """
                    SELECT label.resource_name, label.id
                    FROM label
                    WHERE label.name = 'SD_CHECKED'
                    LIMIT 1
                """
                sd_checked_resource = None
                try:
                    sd_checked_response = ga_service.search(customer_id=customer_id, query=sd_checked_query)
                    for row in sd_checked_response:
                        sd_checked_resource = row.label.resource_name
                        break
                except Exception:
                    pass

                # Create SD_CHECKED label if it doesn't exist
                if not sd_checked_resource:
                    try:
                        label_operation = client.get_type("LabelOperation")
                        label = label_operation.create
                        label.name = "SD_CHECKED"

                        response = label_service.mutate_labels(
                            customer_id=customer_id,
                            operations=[label_operation]
                        )
                        sd_checked_resource = response.results[0].resource_name
                        logger.info(f"Customer {customer_id}: Created SD_CHECKED label")
                    except Exception as e:
                        logger.error(f"Customer {customer_id}: Could not create SD_CHECKED label: {e}")
                        continue

                # Query ad groups WITH SD_DONE label but WITHOUT SD_CHECKED label
                ad_groups_query = """
                    SELECT
                        ad_group.id,
                        ad_group.name,
                        ad_group.resource_name,
                        campaign.id,
                        campaign.name
                    FROM ad_group
                    WHERE campaign.name LIKE 'HS/%'
                    AND ad_group.status = 'ENABLED'
                    AND campaign.status = 'ENABLED'
                """

                # Get all ad groups in HS/ campaigns
                all_ad_groups = []
                try:
                    ag_response = ga_service.search(customer_id=customer_id, query=ad_groups_query)
                    for row in ag_response:
                        all_ad_groups.append({
                            'ad_group_id': str(row.ad_group.id),
                            'ad_group_name': row.ad_group.name,
                            'ad_group_resource': row.ad_group.resource_name,
                            'campaign_id': str(row.campaign.id),
                            'campaign_name': row.campaign.name
                        })
                except Exception as e:
                    logger.warning(f"Customer {customer_id}: Error querying ad groups: {e}")
                    continue

                if not all_ad_groups:
                    logger.info(f"Customer {customer_id}: No ad groups found")
                    continue

                logger.info(f"Customer {customer_id}: Found {len(all_ad_groups)} ad groups")

                # Get ad groups WITH SD_DONE label (in batches)
                ad_groups_with_sd_done = set()
                ad_group_resources = [ag['ad_group_resource'] for ag in all_ad_groups]

                for i in range(0, len(ad_group_resources), batch_size):
                    batch = ad_group_resources[i:i + batch_size]
                    resources_str = ", ".join(f"'{r}'" for r in batch)

                    label_check_query = f"""
                        SELECT ad_group_label.ad_group
                        FROM ad_group_label
                        WHERE ad_group_label.ad_group IN ({resources_str})
                        AND ad_group_label.label = '{sd_done_resource}'
                    """

                    try:
                        label_response = ga_service.search(customer_id=customer_id, query=label_check_query)
                        for row in label_response:
                            ad_groups_with_sd_done.add(row.ad_group_label.ad_group)
                    except Exception as e:
                        logger.warning(f"Customer {customer_id}: Error checking SD_DONE labels: {e}")

                # Get ad groups WITH SD_CHECKED label (in batches)
                ad_groups_with_sd_checked = set()

                for i in range(0, len(ad_group_resources), batch_size):
                    batch = ad_group_resources[i:i + batch_size]
                    resources_str = ", ".join(f"'{r}'" for r in batch)

                    label_check_query = f"""
                        SELECT ad_group_label.ad_group
                        FROM ad_group_label
                        WHERE ad_group_label.ad_group IN ({resources_str})
                        AND ad_group_label.label = '{sd_checked_resource}'
                    """

                    try:
                        label_response = ga_service.search(customer_id=customer_id, query=label_check_query)
                        for row in label_response:
                            ad_groups_with_sd_checked.add(row.ad_group_label.ad_group)
                    except Exception as e:
                        logger.warning(f"Customer {customer_id}: Error checking SD_CHECKED labels: {e}")

                # Filter to ad groups WITH SD_DONE but WITHOUT SD_CHECKED
                unchecked_ad_groups = [
                    ag for ag in all_ad_groups
                    if ag['ad_group_resource'] in ad_groups_with_sd_done
                    and ag['ad_group_resource'] not in ad_groups_with_sd_checked
                ]

                logger.info(f"Customer {customer_id}: {len(unchecked_ad_groups)} ad groups need checking")

                # Check each ad group for SINGLES_DAY ads
                ad_groups_to_check = []
                ad_groups_to_label = []

                for ag in unchecked_ad_groups:
                    # Check if we've reached the limit
                    if limit and stats['ad_groups_checked'] >= limit:
                        logger.info(f"Reached limit of {limit} ad groups checked")
                        break

                    ad_groups_to_check.append(ag)

                # Batch check for SINGLES_DAY ads
                if ad_groups_to_check:
                    for i in range(0, len(ad_groups_to_check), batch_size):
                        batch = ad_groups_to_check[i:i + batch_size]
                        ag_ids = [ag['ad_group_id'] for ag in batch]
                        ag_ids_str = ", ".join(ag_ids)

                        # Query for all ads in these ad groups, then filter in Python
                        all_ads_query = f"""
                            SELECT
                                ad_group_ad.ad_group,
                                ad_group.id,
                                ad_group_ad.ad.responsive_search_ad.headlines
                            FROM ad_group_ad
                            WHERE ad_group.id IN ({ag_ids_str})
                            AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                            AND ad_group_ad.status != REMOVED
                        """

                        ad_groups_with_singles_day = set()
                        try:
                            ad_response = ga_service.search(customer_id=customer_id, query=all_ads_query)
                            for row in ad_response:
                                # Check if any headline contains "SINGLES" (case-insensitive)
                                has_singles = False
                                for headline in row.ad_group_ad.ad.responsive_search_ad.headlines:
                                    if 'SINGLES' in headline.text.upper():
                                        has_singles = True
                                        break

                                if has_singles:
                                    ad_groups_with_singles_day.add(str(row.ad_group.id))
                        except Exception as e:
                            logger.warning(f"Customer {customer_id}: Error checking SINGLES_DAY ads: {e}")

                        # Process batch results
                        for ag in batch:
                            stats['ad_groups_checked'] += 1

                            if ag['ad_group_id'] in ad_groups_with_singles_day:
                                # Has SINGLES_DAY ad - mark as verified
                                stats['ad_groups_verified'] += 1
                                ad_groups_to_label.append(ag)
                            else:
                                # Missing SINGLES_DAY ad - needs repair
                                stats['ad_groups_missing_singles_day'] += 1
                                repair_items.append({
                                    'customer_id': customer_id,
                                    'campaign_id': ag['campaign_id'],
                                    'campaign_name': ag['campaign_name'],
                                    'ad_group_id': ag['ad_group_id'],
                                    'ad_group_name': ag['ad_group_name']
                                })

                            # Check limit after each ad group
                            if limit and stats['ad_groups_checked'] >= limit:
                                break

                # Apply SD_CHECKED label to verified ad groups (in batches)
                if ad_groups_to_label:
                    ad_group_label_service = client.get_service("AdGroupLabelService")

                    for i in range(0, len(ad_groups_to_label), batch_size):
                        batch = ad_groups_to_label[i:i + batch_size]

                        operations = []
                        for ag in batch:
                            operation = client.get_type("AdGroupLabelOperation")
                            ag_label = operation.create
                            ag_label.ad_group = ag['ad_group_resource']
                            ag_label.label = sd_checked_resource
                            operations.append(operation)

                        try:
                            response = ad_group_label_service.mutate_ad_group_labels(
                                customer_id=customer_id,
                                operations=operations
                            )
                            stats['sd_checked_labels_applied'] += len(response.results)
                            logger.info(f"Customer {customer_id}: Applied SD_CHECKED label to {len(response.results)} ad groups")
                        except Exception as e:
                            logger.error(f"Customer {customer_id}: Error applying SD_CHECKED labels: {e}")

                logger.info(f"Customer {customer_id}: Completed - checked {stats['ad_groups_checked']} ad groups")

            except Exception as e:
                logger.error(f"Customer {customer_id}: Unexpected error: {e}", exc_info=True)
                continue

            # Check if we've reached the limit
            if limit and stats['ad_groups_checked'] >= limit:
                break

        # Create repair jobs if needed
        job_ids = []
        if repair_items:
            logger.info(f"Creating repair jobs for {len(repair_items)} ad groups")

            # Split into jobs based on job_chunk_size
            num_chunks = (len(repair_items) + job_chunk_size - 1) // job_chunk_size

            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * job_chunk_size
                end_idx = min(start_idx + job_chunk_size, len(repair_items))
                chunk_data = repair_items[start_idx:end_idx]

                # Create repair job
                job_id = self.create_job(chunk_data, batch_size=batch_size)
                job_ids.append(job_id)
                stats['repair_jobs_created'] += 1
                logger.info(f"Created repair job {job_id} with {len(chunk_data)} items")

                # Automatically start the job
                if background_tasks:
                    background_tasks.add_task(self.process_job, job_id)

        logger.info(f"Checkup completed: {stats}")

        return {
            'status': 'completed',
            'stats': stats,
            'repair_job_ids': job_ids
        }


# Global service instance
thema_ads_service = ThemaAdsService()
