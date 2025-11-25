"""
Fix theme labels on ads with themed URLs across all customers.

Strategy:
1. Find ads with themed URLs (path1 contains theme keywords)
2. Check if ad has correct theme label
3. If incorrect/missing:
   - Check if duplicate ad with correct label exists
   - If yes: Remove this ad
   - If no: Fix the label (remove wrong + add correct)
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
import logging
import psycopg2
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple
from collections import defaultdict
import os
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Add paths
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))
sys.path.insert(0, str(Path(__file__).parent))

# Theme mappings
THEME_KEYWORDS = {
    'black_friday': 'THEME_BF',
    'cyber_monday': 'THEME_CM',
    'sinterklaas': 'THEME_SK',
    'kerstmis': 'THEME_KM',
    'singles_day': 'THEME_SD'
}

THEME_LABELS = set(THEME_KEYWORDS.values())


def get_all_customer_ids():
    """Get all customer IDs from the database."""
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


def detect_theme_from_url(url: str) -> str:
    """
    Detect theme from URL path.

    Args:
        url: Final URL of the ad

    Returns:
        Theme label (e.g., 'THEME_BF') or None if no theme detected
    """
    if not url:
        return None

    # Extract path from URL (after domain)
    # Example: https://www.beslist.nl/products/black_friday/... → black_friday
    url_lower = url.lower()

    for keyword, label in THEME_KEYWORDS.items():
        if keyword in url_lower:
            return label

    return None


def get_ad_content_signature(headlines: List[str], descriptions: List[str]) -> str:
    """Create a signature for ad content to identify duplicates."""
    h_sorted = tuple(sorted(headlines))
    d_sorted = tuple(sorted(descriptions))
    return f"{h_sorted}||{d_sorted}"


def batch_fetch_ad_labels(customer_id: str, ad_ids: List[Tuple[str, str]], ga_service) -> Dict[str, Set[str]]:
    """
    Batch fetch all ad labels.

    Returns:
        Dict mapping "ad_group_id~ad_id" to set of label names
    """
    logger.info(f"Batch fetching labels for {len(ad_ids)} ads...")
    ad_labels = {}

    if not ad_ids:
        return ad_labels

    # Process in batches of 5000
    batch_size = 5000
    for i in range(0, len(ad_ids), batch_size):
        batch = ad_ids[i:i + batch_size]

        # Build IN clause
        ad_resources = [f"'customers/{customer_id}/adGroupAds/{ag_id}~{ad_id}'"
                       for ag_id, ad_id in batch]
        in_clause = ", ".join(ad_resources)

        # Fetch label resources
        query1 = f"""
            SELECT
                ad_group_ad_label.ad_group_ad,
                ad_group_ad_label.label
            FROM ad_group_ad_label
            WHERE ad_group_ad_label.ad_group_ad IN ({in_clause})
        """

        try:
            ad_to_label_resources = {}
            all_label_resources = set()

            response = ga_service.search(customer_id=customer_id, query=query1)
            for row in response:
                ad_resource = row.ad_group_ad_label.ad_group_ad
                key = ad_resource.split('/')[-1]
                label_resource = row.ad_group_ad_label.label

                if key not in ad_to_label_resources:
                    ad_to_label_resources[key] = []
                ad_to_label_resources[key].append(label_resource)
                all_label_resources.add(label_resource)

            # Batch fetch label names
            label_resource_to_name = {}
            if all_label_resources:
                label_resources_list = [f"'{lr}'" for lr in all_label_resources]
                label_in_clause = ", ".join(label_resources_list)

                query2 = f"""
                    SELECT label.resource_name, label.name
                    FROM label
                    WHERE label.resource_name IN ({label_in_clause})
                """

                label_response = ga_service.search(customer_id=customer_id, query=query2)
                for row in label_response:
                    label_resource_to_name[row.label.resource_name] = row.label.name

            # Map label names to ads
            for ad_key, label_resources in ad_to_label_resources.items():
                if ad_key not in ad_labels:
                    ad_labels[ad_key] = set()
                for label_resource in label_resources:
                    label_name = label_resource_to_name.get(label_resource)
                    if label_name:
                        ad_labels[ad_key].add(label_name)

        except Exception as e:
            logger.warning(f"Failed to batch fetch labels: {e}")

    logger.info(f"Fetched labels for {len(ad_labels)} ads with labels")
    return ad_labels


def get_or_create_label(customer_id: str, label_name: str, client) -> str:
    """Get or create a label, return resource name."""
    ga_service = client.get_service("GoogleAdsService")
    query = f"SELECT label.resource_name FROM label WHERE label.name = '{label_name}'"

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            return row.label.resource_name
    except:
        pass

    # Create label
    try:
        label_service = client.get_service("LabelService")
        label_operation = client.get_type("LabelOperation")
        label = label_operation.create
        label.name = label_name

        response = label_service.mutate_labels(
            customer_id=customer_id,
            operations=[label_operation]
        )
        return response.results[0].resource_name
    except Exception as e:
        logger.error(f"Failed to create label {label_name}: {e}")
        return None


def add_ad_label(customer_id: str, ad_group_id: str, ad_id: str, label_resource: str, client):
    """Add label to ad."""
    try:
        ad_label_service = client.get_service("AdGroupAdLabelService")
        ad_label_operation = client.get_type("AdGroupAdLabelOperation")
        ad_label = ad_label_operation.create
        ad_label.ad_group_ad = f"customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}"
        ad_label.label = label_resource

        ad_label_service.mutate_ad_group_ad_labels(
            customer_id=customer_id,
            operations=[ad_label_operation]
        )
        logger.info(f"    ✓ Added label to ad {ad_id}")
    except Exception as e:
        if "ENTITY_ALREADY_EXISTS" not in str(e):
            logger.warning(f"Failed to add label to ad {ad_id}: {e}")


def remove_ad_label(customer_id: str, ad_group_id: str, ad_id: str, label_resource: str, client):
    """Remove label from ad."""
    try:
        ad_label_service = client.get_service("AdGroupAdLabelService")
        ad_label_operation = client.get_type("AdGroupAdLabelOperation")
        ad_label_operation.remove = f"customers/{customer_id}/adGroupAdLabels/{ad_group_id}~{ad_id}~{label_resource.split('/')[-1]}"

        ad_label_service.mutate_ad_group_ad_labels(
            customer_id=customer_id,
            operations=[ad_label_operation]
        )
        logger.info(f"    ✓ Removed label from ad {ad_id}")
    except Exception as e:
        logger.warning(f"Failed to remove label from ad {ad_id}: {e}")


def remove_ad(customer_id: str, ad_resource_name: str, ad_id: str, client):
    """Remove an ad."""
    try:
        ad_service = client.get_service("AdGroupAdService")
        ad_operation = client.get_type("AdGroupAdOperation")
        ad_operation.remove = ad_resource_name

        ad_service.mutate_ad_group_ads(
            customer_id=customer_id,
            operations=[ad_operation]
        )
        logger.info(f"    ✓ Removed ad {ad_id}")
    except Exception as e:
        logger.error(f"    ✗ Failed to remove ad {ad_id}: {e}")


def add_ad_group_label(customer_id: str, ad_group_id: str, label_resource: str, client):
    """Add label to ad group."""
    try:
        ag_label_service = client.get_service("AdGroupLabelService")
        ag_label_operation = client.get_type("AdGroupLabelOperation")
        ag_label = ag_label_operation.create
        ag_label.ad_group = f"customers/{customer_id}/adGroups/{ad_group_id}"
        ag_label.label = label_resource

        ag_label_service.mutate_ad_group_labels(
            customer_id=customer_id,
            operations=[ag_label_operation]
        )
        logger.info(f"  ✓ Added THEME_CORRECTED label to ad group {ad_group_id}")
    except Exception as e:
        if "ENTITY_ALREADY_EXISTS" not in str(e):
            logger.warning(f"Failed to add label to ad group {ad_group_id}: {e}")


def process_single_customer(args):
    """
    Process a single customer to fix theme labels.

    Returns:
        Dict with results
    """
    customer_id, idx, total_customers, dry_run = args

    # Import here for multiprocessing
    from config import load_config_from_env
    from google_ads_client import initialize_client

    config = load_config_from_env()
    client = initialize_client(config.google_ads)
    ga_service = client.get_service("GoogleAdsService")

    result = {
        'customer_id': customer_id,
        'ads_fixed': 0,
        'ads_removed': 0,
        'success': False,
        'error': None,
        'idx': idx
    }

    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"[{idx}/{total_customers}] Processing customer {customer_id}")
        logger.info(f"{'='*80}")

        # Get THEME_CORRECTED label (check if customer already processed)
        corrected_label_query = "SELECT label.resource_name FROM label WHERE label.name = 'THEME_CORRECTED'"
        corrected_label_resource = None
        try:
            label_response = ga_service.search(customer_id=customer_id, query=corrected_label_query)
            for row in label_response:
                corrected_label_resource = row.label.resource_name
                break
        except:
            pass

        # Get ad groups with SD_DONE but without THEME_CORRECTED
        ag_query = """
            SELECT
                ad_group.id,
                ad_group.name,
                campaign.id,
                campaign.name
            FROM ad_group
            WHERE ad_group.status = 'ENABLED'
            AND campaign.status = 'ENABLED'
            AND campaign.name LIKE 'HS/%'
        """

        ag_response = ga_service.search(customer_id=customer_id, query=ag_query)
        all_ad_groups = [(str(row.ad_group.id), row.ad_group.name) for row in ag_response]

        # Filter out already-corrected ad groups
        if corrected_label_resource:
            corrected_ags = set()
            ag_label_query = f"""
                SELECT ad_group_label.ad_group
                FROM ad_group_label
                WHERE ad_group_label.label = '{corrected_label_resource}'
            """
            try:
                response = ga_service.search(customer_id=customer_id, query=ag_label_query)
                for row in response:
                    ag_resource = row.ad_group_label.ad_group
                    ag_id = ag_resource.split('/')[-1]
                    corrected_ags.add(ag_id)
                logger.info(f"Skipping {len(corrected_ags)} ad groups already corrected")
            except:
                pass

            ad_groups = [(ag_id, ag_name) for ag_id, ag_name in all_ad_groups if ag_id not in corrected_ags]
        else:
            ad_groups = all_ad_groups

        if not ad_groups:
            logger.info(f"  No ad groups to process for customer {customer_id}")
            result['success'] = True
            return result

        logger.info(f"Checking {len(ad_groups)} ad groups for theme label issues")

        # Batch fetch all ads
        logger.info(f"Batch fetching ads from {len(ad_groups)} ad groups...")
        all_ads_by_ag = {}
        all_ad_ids = []
        ad_group_names = {ag_id: ag_name for ag_id, ag_name in ad_groups}

        batch_size = 1000
        for i in range(0, len(ad_groups), batch_size):
            batch_ad_groups = ad_groups[i:i + batch_size]
            ag_ids_in_batch = [ag_id for ag_id, _ in batch_ad_groups]

            ag_resources = [f"'customers/{customer_id}/adGroups/{ag_id}'" for ag_id in ag_ids_in_batch]
            in_clause = ", ".join(ag_resources)

            ads_query = f"""
                SELECT
                    ad_group_ad.ad_group,
                    ad_group_ad.ad.id,
                    ad_group_ad.resource_name,
                    ad_group_ad.status,
                    ad_group_ad.ad.final_urls,
                    ad_group_ad.ad.responsive_search_ad.headlines,
                    ad_group_ad.ad.responsive_search_ad.descriptions
                FROM ad_group_ad
                WHERE ad_group_ad.ad_group IN ({in_clause})
                AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.status != REMOVED
            """

            try:
                ads_response = ga_service.search(customer_id=customer_id, query=ads_query)

                for row in ads_response:
                    ag_resource = row.ad_group_ad.ad_group
                    ag_id = ag_resource.split('/')[-1]
                    ad_id = str(row.ad_group_ad.ad.id)
                    rsa = row.ad_group_ad.ad.responsive_search_ad

                    # Get first final URL
                    final_url = row.ad_group_ad.ad.final_urls[0] if row.ad_group_ad.ad.final_urls else None

                    ad_data = {
                        'ad_id': ad_id,
                        'resource_name': row.ad_group_ad.resource_name,
                        'status': str(row.ad_group_ad.status),
                        'final_url': final_url,
                        'headlines': [h.text for h in rsa.headlines] if rsa.headlines else [],
                        'descriptions': [d.text for d in rsa.descriptions] if rsa.descriptions else []
                    }

                    if ag_id not in all_ads_by_ag:
                        all_ads_by_ag[ag_id] = {'name': ad_group_names.get(ag_id, 'Unknown'), 'ads': []}

                    all_ads_by_ag[ag_id]['ads'].append(ad_data)
                    all_ad_ids.append((ag_id, ad_id))

            except Exception as e:
                logger.error(f"Error fetching ads for batch: {e}")
                continue

        logger.info(f"Fetched {len(all_ad_ids)} ads from {len(all_ads_by_ag)} ad groups")

        # Batch fetch labels
        ad_labels_dict = batch_fetch_ad_labels(customer_id, all_ad_ids, ga_service)

        # Process ads for theme issues
        ads_to_fix = []
        ads_to_remove = []
        processed_ad_groups = set()

        for ag_id, ag_data in all_ads_by_ag.items():
            ag_name = ag_data['name']

            # Group ads by content (for duplicate detection)
            content_groups = defaultdict(list)

            for ad_data in ag_data['ads']:
                ad_id = ad_data['ad_id']
                label_key = f"{ag_id}~{ad_id}"
                labels = ad_labels_dict.get(label_key, set())

                # Detect theme from URL
                expected_theme = detect_theme_from_url(ad_data['final_url'])

                if not expected_theme:
                    continue  # No theme in URL, skip

                # Get current theme labels
                current_theme_labels = labels & THEME_LABELS

                # Create content signature for duplicate detection
                signature = get_ad_content_signature(ad_data['headlines'], ad_data['descriptions'])

                ad_info = {
                    'ad_id': ad_id,
                    'resource_name': ad_data['resource_name'],
                    'labels': labels,
                    'expected_theme': expected_theme,
                    'current_theme_labels': current_theme_labels,
                    'signature': signature,
                    'final_url': ad_data['final_url']
                }

                content_groups[signature].append(ad_info)

            # Process each content group
            for signature, ads in content_groups.items():
                # Find ads with issues
                issue_ads = []
                correct_ads = []

                for ad_info in ads:
                    if ad_info['expected_theme'] not in ad_info['current_theme_labels']:
                        # Missing or wrong label
                        issue_ads.append(ad_info)
                    elif ad_info['expected_theme'] in ad_info['current_theme_labels']:
                        # Correctly labeled
                        correct_ads.append(ad_info)

                if not issue_ads:
                    continue

                # If there are correctly labeled duplicates, remove the issue ads
                if correct_ads:
                    for ad_info in issue_ads:
                        logger.info(f"\n  Ad group {ag_id} ({ag_name}):")
                        logger.info(f"    Ad {ad_info['ad_id']} has incorrect/missing theme label")
                        logger.info(f"    URL: {ad_info['final_url']}")
                        logger.info(f"    Expected: {ad_info['expected_theme']}, Current: {ad_info['current_theme_labels']}")
                        logger.info(f"    Action: REMOVE (duplicate with correct label exists)")
                        ads_to_remove.append((ag_id, ad_info))
                        processed_ad_groups.add(ag_id)
                else:
                    # No correct duplicate, fix the first one
                    ad_info = issue_ads[0]
                    logger.info(f"\n  Ad group {ag_id} ({ag_name}):")
                    logger.info(f"    Ad {ad_info['ad_id']} has incorrect/missing theme label")
                    logger.info(f"    URL: {ad_info['final_url']}")
                    logger.info(f"    Expected: {ad_info['expected_theme']}, Current: {ad_info['current_theme_labels']}")
                    logger.info(f"    Action: FIX LABEL")
                    ads_to_fix.append((ag_id, ad_info))
                    processed_ad_groups.add(ag_id)

                    # Remove other duplicates
                    for ad_info in issue_ads[1:]:
                        logger.info(f"\n  Ad group {ag_id} ({ag_name}):")
                        logger.info(f"    Ad {ad_info['ad_id']} is duplicate with wrong label")
                        logger.info(f"    Action: REMOVE")
                        ads_to_remove.append((ag_id, ad_info))

        logger.info(f"\nSummary: {len(ads_to_fix)} ads to fix, {len(ads_to_remove)} ads to remove")

        if not dry_run:
            # Execute fixes
            for ag_id, ad_info in ads_to_fix:
                # Remove wrong labels
                for wrong_label in ad_info['current_theme_labels']:
                    if wrong_label != ad_info['expected_theme']:
                        label_resource = get_or_create_label(customer_id, wrong_label, client)
                        if label_resource:
                            remove_ad_label(customer_id, ag_id, ad_info['ad_id'], label_resource, client)

                # Add correct label
                label_resource = get_or_create_label(customer_id, ad_info['expected_theme'], client)
                if label_resource:
                    add_ad_label(customer_id, ag_id, ad_info['ad_id'], label_resource, client)

                result['ads_fixed'] += 1

            # Execute removals
            for ag_id, ad_info in ads_to_remove:
                remove_ad(customer_id, ad_info['resource_name'], ad_info['ad_id'], client)
                result['ads_removed'] += 1

            # Add THEME_CORRECTED label to processed ad groups
            if processed_ad_groups:
                corrected_label = get_or_create_label(customer_id, "THEME_CORRECTED", client)
                if corrected_label:
                    logger.info(f"\nAdding THEME_CORRECTED label to {len(processed_ad_groups)} ad groups...")
                    for ag_id in processed_ad_groups:
                        add_ad_group_label(customer_id, ag_id, corrected_label, client)
        else:
            result['ads_fixed'] = len(ads_to_fix)
            result['ads_removed'] = len(ads_to_remove)
            logger.info(f"\n[DRY RUN] Would fix {len(ads_to_fix)} ads and remove {len(ads_to_remove)} ads")

        result['success'] = True
        logger.info(f"\n{'Would fix' if dry_run else 'Fixed'} {result['ads_fixed']} ads, {'would remove' if dry_run else 'removed'} {result['ads_removed']} ads")

    except Exception as e:
        logger.error(f"  Failed to process customer {customer_id}: {e}", exc_info=True)
        result['error'] = str(e)

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Fix theme labels on ads with themed URLs (PARALLEL)')
    parser.add_argument('--execute', action='store_true', help='Actually make changes (default is dry-run)')
    parser.add_argument('--customer-limit', type=int, help='Limit number of customers to process (for testing)')
    parser.add_argument('--parallel', type=int, default=3, help='Number of customers to process in parallel (default: 3)')

    args = parser.parse_args()
    dry_run = not args.execute

    if dry_run:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info(f"EXECUTE MODE (PARALLEL: {args.parallel} workers) - Will fix theme labels!")
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
    logger.info(f"Using {args.parallel} parallel workers")

    # Prepare arguments
    customer_args = [
        (customer_id, idx, len(customer_ids), dry_run)
        for idx, customer_id in enumerate(customer_ids, 1)
    ]

    # Process customers in parallel
    total_fixed = 0
    total_removed = 0
    failed_customers = []
    completed = 0

    logger.info("\n" + "=" * 80)
    logger.info("Starting parallel processing...")
    logger.info("=" * 80)

    with ProcessPoolExecutor(max_workers=args.parallel) as executor:
        future_to_customer = {
            executor.submit(process_single_customer, arg): arg[0]
            for arg in customer_args
        }

        for future in as_completed(future_to_customer):
            customer_id = future_to_customer[future]
            completed += 1

            try:
                result = future.result()

                if result['success']:
                    total_fixed += result['ads_fixed']
                    total_removed += result['ads_removed']
                    logger.info(f"✓ Completed customer {result['customer_id']} ({completed}/{len(customer_ids)}) - {result['ads_fixed']} fixed, {result['ads_removed']} removed")
                else:
                    failed_customers.append((result['customer_id'], result['error']))
                    logger.error(f"✗ Failed customer {result['customer_id']} ({completed}/{len(customer_ids)})")

            except Exception as e:
                logger.error(f"✗ Exception processing customer {customer_id}: {e}")
                failed_customers.append((customer_id, str(e)))

    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total customers processed: {len(customer_ids)}")
    logger.info(f"Total ads {'would be fixed' if dry_run else 'fixed'}: {total_fixed}")
    logger.info(f"Total ads {'would be removed' if dry_run else 'removed'}: {total_removed}")

    if failed_customers:
        logger.info(f"\nFailed customers ({len(failed_customers)}):")
        for customer_id, error in failed_customers:
            logger.info(f"  - {customer_id}: {error}")

    logger.info("=" * 80)


if __name__ == '__main__':
    main()
