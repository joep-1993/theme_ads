from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import csv
import io
from datetime import datetime
from pathlib import Path
from backend.database import get_db_connection
from backend.thema_ads_service import thema_ads_service

app = FastAPI(title="Theme Ads - Google Ads Automation", version="1.0.0")

# Mount static files
frontend_dir = Path(__file__).parent.parent / "frontend"
app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict this in production
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    """Serve the frontend HTML."""
    html_file = Path(__file__).parent.parent / "frontend" / "thema-ads.html"
    if html_file.exists():
        return FileResponse(html_file)
    return {
        "status": "running",
        "project": "theme_ads",
        "description": "Google Ads Automation API",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/health")
def health_check():
    return {"status": "healthy", "service": "theme_ads"}


def convert_scientific_notation(value: str) -> str:
    """Convert scientific notation to regular number string.
    Handles both period and comma decimal separators (e.g., 1.76256E+11 or 1,76256E+11).
    """
    if not value:
        return value

    value = value.strip()

    # Check if it's in scientific notation (e.g., 1.76256E+11 or 1,76256E+11)
    if 'E' in value.upper():
        try:
            # Replace comma with period for locales that use comma as decimal separator
            value_normalized = value.replace(',', '.')
            # Convert to float, then to int, then to string (removes scientific notation)
            return str(int(float(value_normalized)))
        except (ValueError, OverflowError):
            # If conversion fails, return original value
            return value

    return value


@app.post("/api/thema-ads/discover")
async def discover_ad_groups(
    background_tasks: BackgroundTasks = None,
    limit: int = None,
    batch_size: int = 5000,
    job_chunk_size: int = 50000
):
    """
    Auto-discover ad groups from Google Ads MCC account.
    Finds all 'Beslist.nl -' accounts, campaigns starting with 'HS/',
    and ad groups without SD_DONE label.

    Args:
        limit: Optional limit on number of ad groups to discover
        batch_size: Batch size for API queries (default: 5000)
        job_chunk_size: Maximum items per job (splits large discoveries into multiple jobs, default: 50000)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Discover parameters: limit={limit}, batch_size={batch_size}, job_chunk_size={job_chunk_size}")

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Load customer IDs from file
        account_ids_file = Path(__file__).parent.parent / "thema_ads_optimized" / "account ids"
        if not account_ids_file.exists():
            raise HTTPException(status_code=500, detail="Account IDs file not found")

        with open(account_ids_file, 'r') as f:
            customer_ids = [line.strip() for line in f if line.strip()]

        logger.info(f"Loaded {len(customer_ids)} customer IDs from account ids file")

        # Get all customer accounts
        ga_service = client.get_service("GoogleAdsService")

        # Build customer list with IDs from file
        beslist_customers = [{'id': cid} for cid in customer_ids]

        logger.info(f"Using {len(beslist_customers)} customer accounts from file")

        # Query ads directly with campaign filter for faster discovery
        input_data = []
        ad_group_map = {}  # Deduplicate: ad_group_resource -> {customer_id, campaign_id, campaign_name, ad_group_id}

        for customer in beslist_customers:
            customer_id = customer['id']
            logger.info(f"Processing customer {customer_id}")

            try:
                # Direct ad query with campaign.name filter (much faster than nested queries)
                ad_query = """
                    SELECT
                        ad_group_ad.ad_group,
                        ad_group.id,
                        ad_group.name,
                        campaign.id,
                        campaign.name
                    FROM ad_group_ad
                    WHERE campaign.name LIKE 'HS/%'
                    AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                    AND ad_group_ad.status != REMOVED
                    AND ad_group.status = 'ENABLED'
                    AND campaign.status = 'ENABLED'
                """

                ad_response = ga_service.search(customer_id=customer_id, query=ad_query)

                # Deduplicate by ad_group (multiple ads per ad group)
                for row in ad_response:
                    ag_resource = row.ad_group_ad.ad_group

                    # Only store first occurrence of each ad group
                    if ag_resource not in ad_group_map:
                        ad_group_map[ag_resource] = {
                            'customer_id': customer_id,
                            'campaign_id': str(row.campaign.id),
                            'campaign_name': row.campaign.name,
                            'ad_group_id': str(row.ad_group.id),
                            'ad_group_resource': ag_resource
                        }

                logger.info(f"  Found {len([ag for ag in ad_group_map.values() if ag['customer_id'] == customer_id])} unique ad groups")

            except Exception as e:
                logger.warning(f"Error processing customer {customer_id}: {e}")
                continue

        # Get all unique ad group resources across all customers
        ad_group_resources = list(ad_group_map.keys())
        logger.info(f"Total unique ad groups across all customers: {len(ad_group_resources)}")

        if not ad_group_resources:
            return {
                "status": "no_ad_groups_found",
                "message": "No ad groups found matching the criteria",
                "total_items": 0,
                "customers_found": len(beslist_customers)
            }

        # Batch check SD_DONE labels (group by customer for API efficiency)
        ag_with_sd_done = set()

        for customer in beslist_customers:
            customer_id = customer['id']

            # Get ad groups for this customer
            customer_ag_resources = [
                ag_resource for ag_resource, ag_data in ad_group_map.items()
                if ag_data['customer_id'] == customer_id
            ]

            if not customer_ag_resources:
                continue

            # Get SD_DONE label resource
            sd_done_query = """
                SELECT label.resource_name
                FROM label
                WHERE label.name = 'SD_DONE'
                LIMIT 1
            """
            try:
                sd_label_response = ga_service.search(customer_id=customer_id, query=sd_done_query)
                sd_done_resource = None
                for row in sd_label_response:
                    sd_done_resource = row.label.resource_name
                    break

                if sd_done_resource:
                    # Batch query in chunks using configured batch_size
                    for i in range(0, len(customer_ag_resources), batch_size):
                        batch = customer_ag_resources[i:i + batch_size]
                        resources_str = ", ".join(f"'{r}'" for r in batch)

                        label_check_query = f"""
                            SELECT ad_group_label.ad_group
                            FROM ad_group_label
                            WHERE ad_group_label.ad_group IN ({resources_str})
                            AND ad_group_label.label = '{sd_done_resource}'
                        """

                        label_response = ga_service.search(customer_id=customer_id, query=label_check_query)
                        for row in label_response:
                            ag_with_sd_done.add(row.ad_group_label.ad_group)

            except Exception as e:
                logger.warning(f"  Could not check SD_DONE labels for customer {customer_id}: {e}")

        # Build input data from ad groups without SD_DONE label
        for ag_resource, ag_data in ad_group_map.items():
            if ag_resource not in ag_with_sd_done:
                input_data.append({
                    'customer_id': ag_data['customer_id'],
                    'campaign_id': ag_data['campaign_id'],
                    'campaign_name': ag_data['campaign_name'],
                    'ad_group_id': ag_data['ad_group_id']
                })

                # Check limit
                if limit and len(input_data) >= limit:
                    logger.info(f"Reached limit of {limit} ad groups")
                    break

        logger.info(f"Discovered {len(input_data)} ad groups to process")

        if not input_data:
            return {
                "status": "no_ad_groups_found",
                "message": "No ad groups found matching the criteria",
                "total_items": 0
            }

        # Split into multiple jobs if needed
        from backend.thema_ads_service import thema_ads_service
        job_ids = []
        total_items = len(input_data)

        # Calculate number of chunks needed
        num_chunks = (total_items + job_chunk_size - 1) // job_chunk_size

        if num_chunks > 1:
            logger.info(f"Splitting {total_items} ad groups into {num_chunks} jobs of max {job_chunk_size} items each")

        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * job_chunk_size
            end_idx = min(start_idx + job_chunk_size, total_items)
            chunk_data = input_data[start_idx:end_idx]

            # Create job for this chunk
            job_id = thema_ads_service.create_job(chunk_data, batch_size=batch_size)
            job_ids.append(job_id)
            logger.info(f"Created job {job_id} with {len(chunk_data)} items (chunk {chunk_idx + 1}/{num_chunks})")

            # Automatically start the job
            if background_tasks:
                background_tasks.add_task(thema_ads_service.process_job, job_id)

        return {
            "job_ids": job_ids,
            "total_items": total_items,
            "jobs_created": len(job_ids),
            "items_per_job": job_chunk_size,
            "status": "processing",
            "customers_found": len(beslist_customers),
            "ad_groups_discovered": total_items
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Discovery failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/thema-ads/upload")
async def upload_csv(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    batch_size: int = Form(7500)
):
    """
    Upload CSV file with customer_id and ad_group_id columns.
    Creates a new job and automatically starts processing.

    Args:
        file: CSV file to upload
        batch_size: Batch size for API queries (default: 7500)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Upload parameters: batch_size={batch_size}")

    try:
        logger.info(f"Receiving file upload: {file.filename}")
        contents = await file.read()
        logger.info(f"File size: {len(contents)} bytes")

        # Try multiple encodings to decode the file
        decoded = None
        encodings = ['utf-8', 'utf-8-sig', 'windows-1252', 'iso-8859-1', 'latin1']
        for encoding in encodings:
            try:
                decoded = contents.decode(encoding)
                logger.info(f"Successfully decoded file using encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue

        if decoded is None:
            raise HTTPException(
                status_code=400,
                detail="Unable to decode file. Please ensure it's a valid CSV file saved with UTF-8 or Windows-1252 encoding."
            )

        # Auto-detect delimiter (comma or semicolon)
        sample = decoded[:1024]  # Check first 1KB
        delimiter = ';' if ';' in sample.split('\n')[0] else ','
        logger.info(f"Using delimiter: '{delimiter}'")

        csv_reader = csv.DictReader(io.StringIO(decoded), delimiter=delimiter)

        # Parse CSV data
        input_data = []
        headers_seen = None
        for row_num, row in enumerate(csv_reader):
            if headers_seen is None:
                headers_seen = list(row.keys())
                logger.info(f"CSV headers found: {headers_seen}")

            if 'customer_id' in row and 'ad_group_id' in row:
                # Convert scientific notation to regular numbers (Excel export issue)
                customer_id = convert_scientific_notation(row['customer_id'])
                ad_group_id = convert_scientific_notation(row['ad_group_id'])

                # Remove dashes from customer_id (Google Ads API requirement)
                customer_id = customer_id.strip().replace('-', '')
                ad_group_id = ad_group_id.strip()

                # Skip empty rows
                if not customer_id or not ad_group_id:
                    continue

                item = {
                    'customer_id': customer_id,
                    'ad_group_id': ad_group_id
                }

                # Add optional campaign info if provided
                if 'campaign_id' in row and row['campaign_id'].strip():
                    campaign_id = convert_scientific_notation(row['campaign_id'])
                    item['campaign_id'] = campaign_id.strip()
                if 'campaign_name' in row and row['campaign_name'].strip():
                    item['campaign_name'] = row['campaign_name'].strip()

                # Add optional ad_group_name if provided (better than ID due to Excel precision loss)
                if 'ad_group_name' in row and row['ad_group_name'].strip():
                    item['ad_group_name'] = row['ad_group_name'].strip()

                input_data.append(item)

        logger.info(f"Parsed {len(input_data)} rows from CSV")

        if not input_data:
            error_msg = f"CSV must contain 'customer_id' and 'ad_group_id' columns. Found headers: {headers_seen}"
            logger.error(error_msg)
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )

        # Create job with batch_size
        logger.info("Creating job in database...")
        job_id = thema_ads_service.create_job(input_data, batch_size=batch_size)
        logger.info(f"Job created with ID: {job_id}, batch_size: {batch_size}")

        # Automatically start the job
        if background_tasks:
            background_tasks.add_task(thema_ads_service.process_job, job_id)
            logger.info(f"Job {job_id} queued for automatic processing")

        return {
            "job_id": job_id,
            "total_items": len(input_data),
            "status": "processing"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/thema-ads/jobs/{job_id}/start")
async def start_job(job_id: int, background_tasks: BackgroundTasks):
    """Start processing a job in the background."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job['status'] == 'running':
            raise HTTPException(status_code=400, detail="Job is already running")

        # Run job in background
        background_tasks.add_task(thema_ads_service.process_job, job_id)

        return {"status": "started", "job_id": job_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/thema-ads/jobs/{job_id}/pause")
async def pause_job(job_id: int):
    """Pause a running job."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        thema_ads_service.pause_job(job_id)

        return {"status": "paused", "job_id": job_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/thema-ads/jobs/{job_id}/resume")
async def resume_job(job_id: int, background_tasks: BackgroundTasks):
    """Resume a paused job."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job['status'] not in ('paused', 'failed'):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot resume job with status '{job['status']}'"
            )

        # Run job in background
        background_tasks.add_task(thema_ads_service.process_job, job_id)

        return {"status": "resumed", "job_id": job_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/thema-ads/jobs/{job_id}")
async def get_job_status(job_id: int):
    """Get detailed status of a specific job."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        return job

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/thema-ads/jobs")
async def list_jobs(limit: int = 20):
    """List all jobs."""
    try:
        jobs = thema_ads_service.list_jobs(limit)
        return {"jobs": jobs}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/thema-ads/jobs/{job_id}")
async def delete_job(job_id: int):
    """Delete a job and all its associated data."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        # Don't allow deleting running jobs
        if job['status'] == 'running':
            raise HTTPException(
                status_code=400,
                detail="Cannot delete a running job. Please pause it first."
            )

        thema_ads_service.delete_job(job_id)

        return {"status": "deleted", "job_id": job_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/thema-ads/jobs/{job_id}/failed-items-csv")
async def download_failed_items(job_id: int):
    """Download CSV of failed and skipped items for a job."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get failed and skipped items
        cur.execute("""
            SELECT customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, status, error_message
            FROM thema_ads_job_items
            WHERE job_id = %s AND status IN ('failed', 'skipped')
            ORDER BY status, customer_id, ad_group_id
        """, (job_id,))

        items = cur.fetchall()
        cur.close()
        conn.close()

        if not items:
            raise HTTPException(status_code=404, detail="No failed or skipped items found for this job")

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(['customer_id', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name', 'status', 'reason', 'error_message'])

        # Write data
        for item in items:
            # Format reason based on status and error message
            if item['status'] == 'skipped':
                if item['error_message'] and 'Already processed' in item['error_message']:
                    reason = "Ad group has 'SD_DONE' label (already processed)"
                elif item['error_message'] and 'No existing ad' in item['error_message']:
                    reason = "Ad group has 0 ads"
                else:
                    reason = item['error_message'] or 'Skipped'
            else:
                reason = item['error_message'] or 'Unknown error'

            writer.writerow([
                item['customer_id'],
                item['campaign_id'] or '',
                item['campaign_name'] or '',
                item['ad_group_id'],
                item['ad_group_name'] or '',
                item['status'],
                reason,
                item['error_message'] or ''
            ])

        # Prepare response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=job_{job_id}_failed_and_skipped_items.csv"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/thema-ads/jobs/{job_id}/successful-items-csv")
async def download_successful_items(job_id: int):
    """Download CSV of successfully processed items for a job."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get successful items
        cur.execute("""
            SELECT customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, new_ad_resource
            FROM thema_ads_job_items
            WHERE job_id = %s AND status = 'successful'
            ORDER BY customer_id, ad_group_id
        """, (job_id,))

        items = cur.fetchall()
        cur.close()
        conn.close()

        if not items:
            raise HTTPException(status_code=404, detail="No successful items found for this job")

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(['customer_id', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name', 'new_ad_resource'])

        # Write data
        for item in items:
            writer.writerow([
                item['customer_id'],
                item['campaign_id'] or '',
                item['campaign_name'] or '',
                item['ad_group_id'],
                item['ad_group_name'] or '',
                item['new_ad_resource'] or ''
            ])

        # Prepare response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=job_{job_id}_successful_items.csv"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/thema-ads/checkup")
async def checkup_ad_groups(
    background_tasks: BackgroundTasks = None,
    limit: int = None,
    batch_size: int = 5000,
    job_chunk_size: int = 50000
):
    """
    Check-up function: Audit ad groups with SD_DONE label, verify SINGLES_DAY ads exist,
    and create repair jobs for missing ads.

    Args:
        limit: Optional limit on number of ad groups to check
        batch_size: Batch size for API queries (default: 5000)
        job_chunk_size: Maximum items per repair job (default: 50000)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Checkup parameters: limit={limit}, batch_size={batch_size}, job_chunk_size={job_chunk_size}")

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Load customer IDs from file
        account_ids_file = Path(__file__).parent.parent / "thema_ads_optimized" / "account ids"
        if not account_ids_file.exists():
            raise HTTPException(status_code=500, detail="Account IDs file not found")

        with open(account_ids_file, 'r') as f:
            customer_ids = [line.strip() for line in f if line.strip()]

        logger.info(f"Loaded {len(customer_ids)} customer IDs from account ids file")

        # Run checkup
        result = await thema_ads_service.checkup_ad_groups(
            client=client,
            customer_ids=customer_ids,
            limit=limit,
            batch_size=batch_size,
            job_chunk_size=job_chunk_size,
            background_tasks=background_tasks
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Checkup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
