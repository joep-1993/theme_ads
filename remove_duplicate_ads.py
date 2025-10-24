"""
Remove duplicate RSAs from ad groups, keeping ads with proper theme labels.

Strategy:
1. For each ad group, find all RSAs
2. Group ads by their content (headlines + descriptions)
3. When duplicates found, keep the one with theme labels (THEME_BF, THEME_CM, etc.)
4. Remove ads without theme labels
"""

import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Initialize Google Ads client
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))
from config import load_config_from_env
from google_ads_client import initialize_client

config = load_config_from_env()
client = initialize_client(config.google_ads)
ga_service = client.get_service("GoogleAdsService")
ad_service = client.get_service("AdGroupAdService")


def get_ad_content_signature(headlines: List[str], descriptions: List[str]) -> str:
    """Create a signature for ad content to identify duplicates."""
    # Sort to handle order variations
    h_sorted = tuple(sorted(headlines))
    d_sorted = tuple(sorted(descriptions))
    return f"{h_sorted}||{d_sorted}"


def batch_fetch_ad_labels(customer_id: str, ad_ids: List[Tuple[str, str]]) -> Dict[str, Set[str]]:
    """
    Batch fetch all ad labels for a list of ads.

    Args:
        customer_id: Google Ads customer ID
        ad_ids: List of (ad_group_id, ad_id) tuples

    Returns:
        Dict mapping "ad_group_id~ad_id" to set of label names
    """
    logger.info(f"Batch fetching labels for {len(ad_ids)} ads...")
    ad_labels = {}

    # Process in batches of 5000
    batch_size = 5000
    for i in range(0, len(ad_ids), batch_size):
        batch = ad_ids[i:i + batch_size]

        # Build IN clause for ad_group_ad resources
        ad_resources = [f"'customers/{customer_id}/adGroupAds/{ag_id}~{ad_id}'"
                       for ag_id, ad_id in batch]
        in_clause = ", ".join(ad_resources)

        # Fetch labels (Google Ads QL doesn't support explicit JOINs)
        # Step 1: Get all label resources for ads in this batch
        query1 = f"""
            SELECT
                ad_group_ad_label.ad_group_ad,
                ad_group_ad_label.label
            FROM ad_group_ad_label
            WHERE ad_group_ad_label.ad_group_ad IN ({in_clause})
        """

        try:
            # Collect label resources per ad
            ad_to_label_resources = {}
            all_label_resources = set()

            response = ga_service.search(customer_id=customer_id, query=query1)
            for row in response:
                ad_resource = row.ad_group_ad_label.ad_group_ad
                key = ad_resource.split('/')[-1]  # Gets "ad_group_id~ad_id"
                label_resource = row.ad_group_ad_label.label

                if key not in ad_to_label_resources:
                    ad_to_label_resources[key] = []
                ad_to_label_resources[key].append(label_resource)
                all_label_resources.add(label_resource)

            # Step 2: Batch fetch label names if we found any labels
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

            # Step 3: Map label names to ads
            for ad_key, label_resources in ad_to_label_resources.items():
                if ad_key not in ad_labels:
                    ad_labels[ad_key] = set()
                for label_resource in label_resources:
                    label_name = label_resource_to_name.get(label_resource)
                    if label_name:
                        ad_labels[ad_key].add(label_name)

        except Exception as e:
            logger.warning(f"Failed to batch fetch labels for batch {i//batch_size}: {e}")

    logger.info(f"Fetched labels for {len(ad_labels)} ads with labels")
    return ad_labels


def get_ad_labels(customer_id: str, ad_group_id: str, ad_id: str) -> Set[str]:
    """Get all labels for an ad (legacy function for compatibility)."""
    # This is now just a fallback - should use batch_fetch_ad_labels instead
    query = f"""
        SELECT label.name
        FROM ad_group_ad_label
        INNER JOIN label ON ad_group_ad_label.label = label.resource_name
        WHERE ad_group_ad_label.ad_group_ad = 'customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}'
    """

    labels = set()
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            labels.add(row.label.name)
    except Exception as e:
        logger.warning(f"Failed to get labels for ad {ad_id}: {e}")

    return labels


def get_or_create_label(customer_id: str, label_name: str) -> str:
    """Get or create a label, return resource name."""
    # Check if label exists
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


def add_ad_group_label(customer_id: str, ad_group_id: str, label_resource: str):
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
        logger.info(f"  ✓ Added label to ad group {ad_group_id}")
    except Exception as e:
        # Ignore if label already exists
        if "ENTITY_ALREADY_EXISTS" not in str(e):
            logger.warning(f"Failed to add label to ad group {ad_group_id}: {e}")


def find_duplicate_ads(customer_id: str, limit: int = None, skip_labeled: bool = True) -> Dict[str, List[Tuple]]:
    """
    Find duplicate ads in all ad groups for a customer.

    Args:
        customer_id: Google Ads customer ID
        limit: Limit number of ad groups to check
        skip_labeled: Skip ad groups with DUPLICATES_CHECKED label

    Returns:
        Dict mapping ad_group_id to list of duplicate sets
        Each duplicate set is (ad_id, resource_name, labels, headlines, descriptions)
    """
    logger.info(f"Scanning customer {customer_id} for duplicate ads...")

    # Get all ad groups
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

    # Filter out already-checked ad groups if requested
    if skip_labeled:
        # Get DUPLICATES_CHECKED label if it exists
        label_query = "SELECT label.resource_name FROM label WHERE label.name = 'DUPLICATES_CHECKED'"
        checked_label_resource = None
        try:
            label_response = ga_service.search(customer_id=customer_id, query=label_query)
            for row in label_response:
                checked_label_resource = row.label.resource_name
                break
        except:
            pass

        if checked_label_resource:
            # Get ad groups with this label
            checked_ags = set()
            ag_label_query = f"""
                SELECT ad_group_label.ad_group
                FROM ad_group_label
                WHERE ad_group_label.label = '{checked_label_resource}'
            """
            try:
                response = ga_service.search(customer_id=customer_id, query=ag_label_query)
                for row in response:
                    # Extract ad group ID from resource name
                    ag_resource = row.ad_group_label.ad_group
                    ag_id = ag_resource.split('/')[-1]
                    checked_ags.add(ag_id)

                logger.info(f"Skipping {len(checked_ags)} ad groups already checked")
            except:
                pass

            # Filter out checked ad groups
            ad_groups = [(ag_id, ag_name) for ag_id, ag_name in all_ad_groups if ag_id not in checked_ags]
        else:
            ad_groups = all_ad_groups
    else:
        ad_groups = all_ad_groups

    if limit:
        ad_groups = ad_groups[:limit]

    logger.info(f"Checking {len(ad_groups)} ad groups for duplicates")

    # OPTIMIZATION: Batch fetch all ads across all ad groups
    logger.info(f"Batch fetching all ads from {len(ad_groups)} ad groups...")
    all_ads_by_ag = {}
    all_ad_ids = []

    # Create mapping for quick lookup
    ad_group_names = {ag_id: ag_name for ag_id, ag_name in ad_groups}

    # Process in batches of 1000 ad groups to avoid query size limits
    batch_size = 1000
    for i in range(0, len(ad_groups), batch_size):
        batch_ad_groups = ad_groups[i:i + batch_size]
        ag_ids_in_batch = [ag_id for ag_id, _ in batch_ad_groups]

        # Build IN clause for ad groups
        ag_resources = [f"'customers/{customer_id}/adGroups/{ag_id}'" for ag_id in ag_ids_in_batch]
        in_clause = ", ".join(ag_resources)

        # Fetch all ads for this batch of ad groups
        ads_query = f"""
            SELECT
                ad_group_ad.ad_group,
                ad_group_ad.ad.id,
                ad_group_ad.resource_name,
                ad_group_ad.status,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions
            FROM ad_group_ad
            WHERE ad_group_ad.ad_group IN ({in_clause})
            AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
            AND ad_group_ad.status != REMOVED
        """

        try:
            logger.info(f"Fetching ads for batch {i//batch_size + 1}/{(len(ad_groups) + batch_size - 1)//batch_size} ({len(batch_ad_groups)} ad groups)...")
            ads_response = ga_service.search(customer_id=customer_id, query=ads_query)

            for row in ads_response:
                # Extract ad_group_id from resource
                ag_resource = row.ad_group_ad.ad_group
                ag_id = ag_resource.split('/')[-1]
                ad_id = str(row.ad_group_ad.ad.id)
                rsa = row.ad_group_ad.ad.responsive_search_ad

                ad_data = {
                    'ad_id': ad_id,
                    'resource_name': row.ad_group_ad.resource_name,
                    'status': str(row.ad_group_ad.status),
                    'headlines': [h.text for h in rsa.headlines] if rsa.headlines else [],
                    'descriptions': [d.text for d in rsa.descriptions] if rsa.descriptions else []
                }

                # Initialize ad group if not exists
                if ag_id not in all_ads_by_ag:
                    all_ads_by_ag[ag_id] = {'name': ad_group_names.get(ag_id, 'Unknown'), 'ads': []}

                all_ads_by_ag[ag_id]['ads'].append(ad_data)
                all_ad_ids.append((ag_id, ad_id))

        except Exception as e:
            logger.error(f"Error batch fetching ads for batch {i//batch_size + 1}: {e}")
            continue

    logger.info(f"Fetched {len(all_ad_ids)} ads from {len(all_ads_by_ag)} ad groups")

    # OPTIMIZATION: Batch fetch all labels at once
    ad_labels_dict = batch_fetch_ad_labels(customer_id, all_ad_ids)

    # Now process duplicates with prefetched labels
    duplicates_by_ag = {}
    total_duplicates = 0

    for ag_id, ag_data in all_ads_by_ag.items():
        ag_name = ag_data['name']
        content_groups = defaultdict(list)

        for ad_data in ag_data['ads']:
            ad_id = ad_data['ad_id']

            # Get labels from prefetched dictionary
            label_key = f"{ag_id}~{ad_id}"
            labels = ad_labels_dict.get(label_key, set())

            # Create content signature
            signature = get_ad_content_signature(ad_data['headlines'], ad_data['descriptions'])

            content_groups[signature].append({
                'ad_id': ad_id,
                'resource_name': ad_data['resource_name'],
                'status': ad_data['status'],
                'labels': labels,
                'headlines': ad_data['headlines'],
                'descriptions': ad_data['descriptions']
            })

        # Find groups with duplicates
        duplicate_groups = [ads for ads in content_groups.values() if len(ads) > 1]

        if duplicate_groups:
            duplicates_by_ag[ag_id] = {
                'name': ag_name,
                'duplicate_groups': duplicate_groups
            }
            total_duplicates += len(duplicate_groups)
            logger.info(f"  Ad group {ag_id} ({ag_name}): Found {len(duplicate_groups)} duplicate set(s)")

    logger.info(f"Total: Found {total_duplicates} duplicate sets across {len(duplicates_by_ag)} ad groups")
    return duplicates_by_ag


def remove_duplicate_ads(customer_id: str, duplicates_by_ag: Dict, dry_run: bool = True, add_labels: bool = True):
    """
    Remove duplicate ads, keeping the one with proper theme labels.

    Priority for keeping:
    1. Ad with most theme labels (THEME_BF, THEME_CM, THEME_SK, THEME_KM, THEME_SD)
    2. Ad with any theme label
    3. Ad with ENABLED status
    4. First ad encountered

    Args:
        customer_id: Google Ads customer ID
        duplicates_by_ag: Dict of duplicate ad groups
        dry_run: If True, only show what would be done
        add_labels: If True, add DUPLICATES_CHECKED label to processed ad groups
    """
    logger.info(f"Processing duplicates (dry_run={dry_run})...")

    theme_labels = {'THEME_BF', 'THEME_CM', 'THEME_SK', 'THEME_KM', 'THEME_SD'}

    # Get or create DUPLICATES_CHECKED label
    checked_label_resource = None
    if add_labels and not dry_run:
        checked_label_resource = get_or_create_label(customer_id, "DUPLICATES_CHECKED")

    total_removed = 0
    processed_ad_groups = []

    for ag_id, ag_info in duplicates_by_ag.items():
        ag_name = ag_info['name']

        for group_idx, duplicate_group in enumerate(ag_info['duplicate_groups']):
            logger.info(f"\n  Ad group {ag_id} ({ag_name}) - Duplicate set {group_idx + 1}:")

            # Score each ad for priority
            scored_ads = []
            for ad in duplicate_group:
                theme_label_count = len([l for l in ad['labels'] if l in theme_labels])
                has_any_theme = any(l in theme_labels for l in ad['labels'])
                is_enabled = ad['status'] == 'ENABLED'

                score = (theme_label_count * 100) + (has_any_theme * 10) + (is_enabled * 1)
                scored_ads.append((score, ad))

            # Sort by score descending - highest score = keep
            scored_ads.sort(reverse=True, key=lambda x: x[0])

            # Keep the first (highest scored), remove the rest
            to_keep = scored_ads[0][1]
            to_remove = [ad for score, ad in scored_ads[1:]]

            logger.info(f"    KEEP: Ad {to_keep['ad_id']} (Status: {to_keep['status']}, Labels: {to_keep['labels']})")
            logger.info(f"         Headlines: {to_keep['headlines'][:3]}...")

            for ad in to_remove:
                logger.info(f"    REMOVE: Ad {ad['ad_id']} (Status: {ad['status']}, Labels: {ad['labels']})")

                if not dry_run:
                    try:
                        # Remove the ad
                        ad_group_ad_operation = client.get_type("AdGroupAdOperation")
                        ad_group_ad_operation.remove = ad['resource_name']

                        response = ad_service.mutate_ad_group_ads(
                            customer_id=customer_id,
                            operations=[ad_group_ad_operation]
                        )
                        logger.info(f"      ✓ Removed ad {ad['ad_id']}")
                        total_removed += 1
                    except Exception as e:
                        logger.error(f"      ✗ Failed to remove ad {ad['ad_id']}: {e}")
                else:
                    logger.info(f"      [DRY RUN] Would remove ad {ad['ad_id']}")
                    total_removed += 1

        # Track this ad group as processed
        processed_ad_groups.append(ag_id)

    # Add DUPLICATES_CHECKED label to processed ad groups
    if add_labels and not dry_run and checked_label_resource and processed_ad_groups:
        logger.info(f"\nAdding DUPLICATES_CHECKED label to {len(processed_ad_groups)} ad groups...")
        for ag_id in processed_ad_groups:
            add_ad_group_label(customer_id, ag_id, checked_label_resource)

    if dry_run:
        logger.info(f"\n[DRY RUN] Would remove {total_removed} duplicate ads")
    else:
        logger.info(f"\nRemoved {total_removed} duplicate ads")
        if add_labels:
            logger.info(f"Labeled {len(processed_ad_groups)} ad groups as DUPLICATES_CHECKED")

    return total_removed


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Remove duplicate RSAs from ad groups')
    parser.add_argument('--customer-id', required=True, help='Google Ads customer ID')
    parser.add_argument('--limit', type=int, help='Limit number of ad groups to check (for testing)')
    parser.add_argument('--execute', action='store_true', help='Actually remove ads (default is dry-run)')
    parser.add_argument('--no-skip-labeled', action='store_true', help='Check all ad groups, even those already checked')
    parser.add_argument('--no-label', action='store_true', help='Do not add DUPLICATES_CHECKED label after processing')

    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info("EXECUTE MODE - Will remove duplicate ads!")
        logger.info("=" * 80)
        response = input("Are you sure you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborted")
            return

    # Find duplicates
    skip_labeled = not args.no_skip_labeled
    duplicates_by_ag = find_duplicate_ads(args.customer_id, limit=args.limit, skip_labeled=skip_labeled)

    if not duplicates_by_ag:
        logger.info("No duplicates found!")
        return

    # Remove duplicates
    add_labels = not args.no_label
    removed_count = remove_duplicate_ads(args.customer_id, duplicates_by_ag, dry_run=dry_run, add_labels=add_labels)

    logger.info("\n" + "=" * 80)
    logger.info(f"Summary: {'Would remove' if dry_run else 'Removed'} {removed_count} duplicate ads")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
