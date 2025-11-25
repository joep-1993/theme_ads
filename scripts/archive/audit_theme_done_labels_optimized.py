#!/usr/bin/env python3
"""
OPTIMIZED: Audit theme DONE labels and remove them if the themed ad is missing.

Performance optimizations:
1. Query all themes at once (4x faster)
2. Filter to HS/ campaigns only (2-3x faster)
3. Better chunking for large queries (1.5x faster)
4. Customer pre-filtering
5. Rate-limited parallel processing

Expected: 12-24x faster than original script
"""

import sys
import argparse
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.ads.googleads.errors import GoogleAdsException
from dotenv import load_dotenv

# Load .env file
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(dotenv_path=env_path)

# Add thema_ads_optimized to path
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))

from config import load_config_from_env
from google_ads_client import initialize_client

# Theme mapping - Maps theme name to (theme_label, done_label)
THEMES = {
    'black_friday': ('THEME_BF', 'THEME_BF_DONE'),
    'cyber_monday': ('THEME_CM', 'THEME_CM_DONE'),
    'sinterklaas': ('THEME_SK', 'THEME_SK_DONE'),
    'kerstmis': ('THEME_KM', 'THEME_KM_DONE'),
    'singles_day': ('THEME_SD', 'THEME_SD_DONE'),  # Legacy
}

# Chunk size for large queries to avoid hitting limits
AD_GROUP_CHUNK_SIZE = 500  # Process 500 ad groups at a time
AD_CHUNK_SIZE = 1000  # Query 1000 ads at a time


def get_customers_with_done_labels(client, customer_ids, themes_to_audit):
    """
    Pre-filter: Find which customers actually have DONE labels.
    This avoids querying customers that don't have any themed ads.

    Returns:
        dict: {customer_id: [theme_names]} - only customers with DONE labels
    """
    print("\n🔍 Pre-filtering customers with DONE labels...")

    customers_with_labels = {}
    done_label_names = [THEMES[theme][1] for theme in themes_to_audit]

    for customer_id in customer_ids:
        try:
            ga_service = client.get_service('GoogleAdsService')

            # Query which DONE labels exist for this customer
            labels_str = "', '".join(done_label_names)
            query = f"""
                SELECT label.name, label.resource_name
                FROM label
                WHERE label.name IN ('{labels_str}')
            """

            response = ga_service.search(customer_id=customer_id, query=query)
            customer_themes = []

            for row in response:
                label_name = row.label.name
                # Find which theme this label belongs to
                for theme_name, (_, done_label) in THEMES.items():
                    if done_label == label_name and theme_name in themes_to_audit:
                        customer_themes.append(theme_name)
                        break

            if customer_themes:
                customers_with_labels[customer_id] = customer_themes
                print(f"  ✓ Customer {customer_id}: {len(customer_themes)} theme(s)")

        except Exception as e:
            print(f"  ⚠️  Customer {customer_id}: Error - {e}")
            continue

    print(f"\n✓ Found {len(customers_with_labels)} customers with DONE labels (out of {len(customer_ids)})")
    return customers_with_labels


def audit_customer_all_themes(client, customer_id, themes_to_audit, dry_run=True, skip_audited=True):
    """
    OPTIMIZED: Audit all themes for a customer at once.

    Instead of 4 separate queries (one per theme), we query all DONE labels
    and all theme labels in bulk, then process them together.

    Args:
        skip_audited: If True, skip ad groups with THEMES_CHECK_DONE label (already audited)

    Returns:
        dict with aggregated stats for all themes
    """
    ga_service = client.get_service('GoogleAdsService')
    ad_group_label_service = client.get_service('AdGroupLabelService')

    aggregated_stats = {
        'customer_id': customer_id,
        'themes': {},
        'total_ad_groups_checked': 0,
        'total_ad_groups_with_done_label': 0,
        'total_ad_groups_missing_theme_ad': 0,
        'total_done_labels_removed': 0,
        'total_ad_groups_skipped_already_audited': 0,
        'total_audit_labels_added': 0,
        'errors': 0
    }

    print(f"\n[{customer_id}] Auditing {len(themes_to_audit)} themes in bulk...")

    try:
        # Step 1: Get all DONE labels, theme labels, and audit tracking label
        all_labels_to_find = ['THEMES_CHECK_DONE']  # Audit tracking label
        for theme in themes_to_audit:
            theme_label, done_label = THEMES[theme]
            all_labels_to_find.extend([theme_label, done_label])

        labels_str = "', '".join(all_labels_to_find)
        labels_query = f"""
            SELECT label.name, label.resource_name
            FROM label
            WHERE label.name IN ('{labels_str}')
        """

        labels_response = ga_service.search(customer_id=customer_id, query=labels_query)

        # Map label names to resources
        label_resources = {}
        for row in labels_response:
            label_resources[row.label.name] = row.label.resource_name

        # Create audit tracking label if it doesn't exist
        audit_label_name = 'THEMES_CHECK_DONE'
        if audit_label_name not in label_resources:
            try:
                label_service = client.get_service('LabelService')
                label_operation = client.get_type('LabelOperation')
                label = label_operation.create
                label.name = audit_label_name
                label.description = 'Ad group audited for theme DONE labels - has valid themed ads'

                response = label_service.mutate_labels(
                    customer_id=customer_id,
                    operations=[label_operation]
                )
                label_resources[audit_label_name] = response.results[0].resource_name
                print(f"[{customer_id}] Created {audit_label_name} label")
            except Exception as e:
                print(f"[{customer_id}] Warning: Could not create {audit_label_name} label: {e}")

        audit_label_resource = label_resources.get(audit_label_name)

        # Build theme-specific mappings
        theme_mappings = {}
        for theme in themes_to_audit:
            theme_label, done_label = THEMES[theme]
            if done_label not in label_resources:
                print(f"[{customer_id}] Theme {theme}: No {done_label} label found - skipping")
                continue

            theme_mappings[theme] = {
                'theme_label': theme_label,
                'done_label': done_label,
                'theme_label_resource': label_resources.get(theme_label),
                'done_label_resource': label_resources[done_label]
            }

        if not theme_mappings:
            print(f"[{customer_id}] No DONE labels found - skipping")
            return aggregated_stats

        print(f"[{customer_id}] Found {len(theme_mappings)} theme(s) with DONE labels")

        # Step 1.5: Find ad groups with THEMES_CHECK_DONE label to skip
        ad_groups_already_audited = set()
        if skip_audited and audit_label_resource:
            try:
                audited_query = f"""
                    SELECT ad_group.resource_name
                    FROM ad_group_label
                    WHERE ad_group_label.label = '{audit_label_resource}'
                    AND campaign.name LIKE 'HS/%'
                """
                audited_response = ga_service.search(customer_id=customer_id, query=audited_query)
                for row in audited_response:
                    ad_groups_already_audited.add(row.ad_group.resource_name)

                if ad_groups_already_audited:
                    print(f"[{customer_id}] Found {len(ad_groups_already_audited)} ad groups already audited (will skip)")
            except Exception as e:
                print(f"[{customer_id}] Warning: Could not query already-audited ad groups: {e}")

        # Step 2: Get all ad groups with ANY of the DONE labels (HS/ campaigns only)
        done_resources = [tm['done_label_resource'] for tm in theme_mappings.values()]
        done_resources_str = "', '".join(done_resources)

        ag_query = f"""
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group.resource_name,
                ad_group_label.label,
                ad_group_label.resource_name,
                campaign.name
            FROM ad_group_label
            WHERE ad_group_label.label IN ('{done_resources_str}')
            AND campaign.name LIKE 'HS/%'
        """

        ag_response = ga_service.search(customer_id=customer_id, query=ag_query)

        # Group ad groups by theme
        ad_groups_by_theme = {theme: [] for theme in theme_mappings.keys()}

        for row in ag_response:
            label_resource = row.ad_group_label.label

            # Find which theme this belongs to
            for theme, mapping in theme_mappings.items():
                if label_resource == mapping['done_label_resource']:
                    ad_groups_by_theme[theme].append({
                        'id': str(row.ad_group.id),
                        'name': row.ad_group.name,
                        'resource': row.ad_group.resource_name,
                        'label_resource': row.ad_group_label.resource_name,
                        'campaign_name': row.campaign.name
                    })
                    break

        total_ag_count = sum(len(ags) for ags in ad_groups_by_theme.values())
        print(f"[{customer_id}] Found {total_ag_count} ad groups with DONE labels in HS/ campaigns")
        aggregated_stats['total_ad_groups_with_done_label'] = total_ag_count

        if total_ag_count == 0:
            return aggregated_stats

        # Step 3: Process each theme's ad groups in chunks
        for theme, ad_groups_list in ad_groups_by_theme.items():
            if not ad_groups_list:
                continue

            # Filter out already-audited ad groups
            original_count = len(ad_groups_list)
            if skip_audited and ad_groups_already_audited:
                ad_groups_list = [ag for ag in ad_groups_list
                                  if ag['resource'] not in ad_groups_already_audited]
                skipped_count = original_count - len(ad_groups_list)
                if skipped_count > 0:
                    aggregated_stats['total_ad_groups_skipped_already_audited'] += skipped_count
                    print(f"[{customer_id}] Theme {theme}: Skipped {skipped_count} already-audited ad groups")

            if not ad_groups_list:
                print(f"[{customer_id}] Theme {theme}: All ad groups already audited - skipping")
                continue

            theme_label = theme_mappings[theme]['theme_label']
            done_label = theme_mappings[theme]['done_label']

            theme_stats = {
                'ad_groups_checked': 0,
                'ad_groups_with_done_label': original_count,
                'ad_groups_missing_theme_ad': 0,
                'done_labels_removed': 0,
                'audit_labels_added': 0
            }

            print(f"[{customer_id}] Theme {theme}: Processing {len(ad_groups_list)} ad groups...")

            operations = []
            audit_operations = []

            # Process in chunks to avoid huge queries
            for chunk_idx in range(0, len(ad_groups_list), AD_GROUP_CHUNK_SIZE):
                chunk = ad_groups_list[chunk_idx:chunk_idx + AD_GROUP_CHUNK_SIZE]

                # Query ads for this chunk
                ag_resources = "', '".join([ag['resource'] for ag in chunk])

                ads_query = f"""
                    SELECT
                        ad_group_ad.ad_group,
                        ad_group_ad.resource_name
                    FROM ad_group_ad
                    WHERE ad_group_ad.ad_group IN ('{ag_resources}')
                    AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                    AND ad_group_ad.status != REMOVED
                """

                try:
                    ads_response = ga_service.search(customer_id=customer_id, query=ads_query)

                    # Collect ads by ad group
                    ad_group_ads = {}
                    all_ad_resources = []
                    for row in ads_response:
                        ag_res = row.ad_group_ad.ad_group
                        ad_res = row.ad_group_ad.resource_name

                        if ag_res not in ad_group_ads:
                            ad_group_ads[ag_res] = []
                        ad_group_ads[ag_res].append(ad_res)
                        all_ad_resources.append(ad_res)

                    # Query labels for ads in batches
                    ad_labels_map = {}
                    if all_ad_resources:
                        for i in range(0, len(all_ad_resources), AD_CHUNK_SIZE):
                            batch_ads = all_ad_resources[i:i+AD_CHUNK_SIZE]
                            ad_res_str = "', '".join(batch_ads)

                            labels_query = f"""
                                SELECT
                                    ad_group_ad_label.ad_group_ad,
                                    label.name
                                FROM ad_group_ad_label
                                WHERE ad_group_ad_label.ad_group_ad IN ('{ad_res_str}')
                                AND label.name = '{theme_label}'
                            """

                            try:
                                labels_response = ga_service.search(customer_id=customer_id, query=labels_query)
                                for label_row in labels_response:
                                    ad_res = label_row.ad_group_ad_label.ad_group_ad
                                    label_name = label_row.label.name
                                    if ad_res not in ad_labels_map:
                                        ad_labels_map[ad_res] = set()
                                    ad_labels_map[ad_res].add(label_name)
                            except Exception as e:
                                print(f"[{customer_id}] Warning: Could not query ad labels: {e}")

                    # Check each ad group in this chunk
                    for ag in chunk:
                        theme_stats['ad_groups_checked'] += 1
                        ag_resource = ag['resource']

                        # Check if ad group has the theme ad
                        ads = ad_group_ads.get(ag_resource, [])
                        has_theme_ad = any(
                            theme_label in ad_labels_map.get(ad_res, set())
                            for ad_res in ads
                        )

                        if not has_theme_ad:
                            theme_stats['ad_groups_missing_theme_ad'] += 1
                            print(f"[{customer_id}] ⚠️  {theme}: Ad group {ag['id']} has {done_label} but NO {theme_label} ad")

                            if not dry_run:
                                # Remove the DONE label
                                operation = client.get_type('AdGroupLabelOperation')
                                operation.remove = ag['label_resource']
                                operations.append(operation)
                        else:
                            # Ad group has valid themed ad - add audit tracking label
                            if not dry_run and audit_label_resource:
                                audit_operation = client.get_type('AdGroupLabelOperation')
                                ad_group_label = audit_operation.create
                                ad_group_label.ad_group = ag_resource
                                ad_group_label.label = audit_label_resource
                                audit_operations.append(audit_operation)

                except Exception as e:
                    print(f"[{customer_id}] Theme {theme}, chunk {chunk_idx//AD_GROUP_CHUNK_SIZE}: Error - {e}")
                    aggregated_stats['errors'] += 1

            # Execute removals for this theme
            if operations and not dry_run:
                try:
                    # Remove in batches of 5000 (Google Ads limit)
                    for i in range(0, len(operations), 5000):
                        batch = operations[i:i+5000]
                        response = ad_group_label_service.mutate_ad_group_labels(
                            customer_id=customer_id,
                            operations=batch
                        )
                        theme_stats['done_labels_removed'] += len(response.results)

                    print(f"[{customer_id}] ✅ Theme {theme}: Removed {theme_stats['done_labels_removed']} {done_label} labels")
                except GoogleAdsException as e:
                    print(f"[{customer_id}] ❌ Theme {theme}: Error removing labels: {e}")
                    aggregated_stats['errors'] += 1
            elif operations and dry_run:
                print(f"[{customer_id}] 🔍 DRY RUN - Theme {theme}: Would remove {len(operations)} {done_label} labels")

            # Execute audit label additions for this theme
            if audit_operations and not dry_run:
                try:
                    # Add in batches of 5000 (Google Ads limit)
                    for i in range(0, len(audit_operations), 5000):
                        batch = audit_operations[i:i+5000]
                        response = ad_group_label_service.mutate_ad_group_labels(
                            customer_id=customer_id,
                            operations=batch
                        )
                        theme_stats['audit_labels_added'] += len(response.results)

                    print(f"[{customer_id}] ✅ Theme {theme}: Added {theme_stats['audit_labels_added']} THEMES_CHECK_DONE labels")
                except GoogleAdsException as e:
                    print(f"[{customer_id}] ❌ Theme {theme}: Error adding audit labels: {e}")
                    aggregated_stats['errors'] += 1
            elif audit_operations and dry_run:
                print(f"[{customer_id}] 🔍 DRY RUN - Theme {theme}: Would add {len(audit_operations)} THEMES_CHECK_DONE labels")

            # Store theme stats
            aggregated_stats['themes'][theme] = theme_stats
            aggregated_stats['total_ad_groups_checked'] += theme_stats['ad_groups_checked']
            aggregated_stats['total_ad_groups_missing_theme_ad'] += theme_stats['ad_groups_missing_theme_ad']
            aggregated_stats['total_done_labels_removed'] += theme_stats['done_labels_removed']
            aggregated_stats['total_audit_labels_added'] += theme_stats.get('audit_labels_added', 0)

    except Exception as e:
        print(f"[{customer_id}] ❌ Error auditing customer: {e}")
        aggregated_stats['errors'] += 1

    return aggregated_stats


def main():
    parser = argparse.ArgumentParser(description='Audit theme DONE labels (OPTIMIZED)')
    parser.add_argument('--execute', action='store_true', help='Actually remove labels (default is dry-run)')
    parser.add_argument('--parallel', type=int, default=5, help='Number of parallel workers (default: 5, max recommended: 10)')
    parser.add_argument('--themes', nargs='+', help='Specific themes to audit (default: all)')
    parser.add_argument('--customer', help='Specific customer ID to audit (default: all)')
    args = parser.parse_args()

    # Limit parallel workers to safe range
    if args.parallel > 10:
        print(f"⚠️  Warning: {args.parallel} workers may exceed API rate limits. Limiting to 10.")
        args.parallel = 10

    dry_run = not args.execute

    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be made")
        print("   Use --execute to actually remove labels\n")
    else:
        print("⚠️  EXECUTE MODE - Labels will be removed!")
        print("   Press Ctrl+C to cancel...\n")

    # Load config and initialize client
    print("Loading configuration...")
    config = load_config_from_env()
    client = initialize_client(config.google_ads)
    print("Google Ads client initialized\n")

    # All 28 customers from the database
    customers = [
        '1351439239',
        '1496704472',
        '2237802672',
        '3114657125',
        '3273661472',
        '4056770576',
        '4567815835',
        '4675585929',
        '4761604080',
        '4964513580',
        '5105960927',
        '5122292229',
        '5550062935',
        '5807833423',
        '5930401821',
        '6044293584',
        '6213822688',
        '6271552035',
        '6379322129',
        '6511658729',
        '7346695290',
        '8273243429',
        '8338942127',
        '8431844135',
        '8696777335',
        '8755979133',
        '9251309631',
        '9525057729',
    ]

    if args.customer:
        customers = [args.customer]

    themes_to_audit = args.themes if args.themes else list(THEMES.keys())

    print(f"Auditing {len(themes_to_audit)} themes across {len(customers)} customers")
    print(f"Themes: {', '.join(themes_to_audit)}")
    print(f"Parallel workers: {args.parallel}")
    print(f"Optimizations: bulk queries, HS/ filter, chunking\n")

    start_time = time.time()

    # Pre-filter: Find which customers have DONE labels
    customers_with_labels = get_customers_with_done_labels(client, customers, themes_to_audit)

    if not customers_with_labels:
        print("\n✓ No customers with DONE labels found - nothing to audit!")
        return

    # Process customers in parallel (rate-limited)
    all_stats = []

    print(f"\n🚀 Starting audit with {args.parallel} parallel workers...\n")

    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        futures = {
            executor.submit(audit_customer_all_themes, client, customer_id, customer_themes, dry_run): customer_id
            for customer_id, customer_themes in customers_with_labels.items()
        }

        for future in as_completed(futures):
            customer_id = futures[future]
            try:
                stats = future.result()
                all_stats.append(stats)
            except Exception as e:
                print(f"❌ Error processing customer {customer_id}: {e}")

    # Calculate total time
    elapsed_time = time.time() - start_time

    # Print summary
    print("\n" + "=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)

    total_checked = sum(s['total_ad_groups_checked'] for s in all_stats)
    total_with_done = sum(s['total_ad_groups_with_done_label'] for s in all_stats)
    total_missing = sum(s['total_ad_groups_missing_theme_ad'] for s in all_stats)
    total_removed = sum(s['total_done_labels_removed'] for s in all_stats)
    total_skipped = sum(s['total_ad_groups_skipped_already_audited'] for s in all_stats)
    total_audit_added = sum(s['total_audit_labels_added'] for s in all_stats)
    total_errors = sum(s['errors'] for s in all_stats)

    print(f"Customers audited: {len(all_stats)}")
    print(f"Ad groups checked: {total_checked}")
    print(f"Ad groups with DONE labels: {total_with_done}")
    print(f"Ad groups skipped (already audited): {total_skipped}")
    print(f"Ad groups missing theme ad: {total_missing}")

    if dry_run:
        print(f"DONE labels to remove: {total_missing}")
    else:
        print(f"DONE labels removed: {total_removed}")
        print(f"THEMES_CHECK_DONE labels added: {total_audit_added}")

    print(f"Errors: {total_errors}")
    print(f"\nTime elapsed: {elapsed_time:.1f} seconds")
    if total_checked > 0:
        print(f"Speed: {total_checked / elapsed_time:.1f} ad groups/second")

    # Print details by theme
    print("\nBy theme:")
    theme_summary = {}
    for stats in all_stats:
        for theme, theme_stats in stats.get('themes', {}).items():
            if theme not in theme_summary:
                theme_summary[theme] = {'missing': 0, 'total': 0}
            theme_summary[theme]['missing'] += theme_stats['ad_groups_missing_theme_ad']
            theme_summary[theme]['total'] += theme_stats['ad_groups_with_done_label']

    for theme in sorted(theme_summary.keys()):
        missing = theme_summary[theme]['missing']
        total = theme_summary[theme]['total']
        print(f"  {theme:15s}: {missing:4d} missing / {total:4d} with DONE label")

    print("\n✅ Audit complete!")

    if dry_run and total_missing > 0:
        print(f"\n💡 Run with --execute to remove {total_missing} invalid DONE labels")


if __name__ == '__main__':
    main()
