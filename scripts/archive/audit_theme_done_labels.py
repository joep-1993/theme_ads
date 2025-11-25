#!/usr/bin/env python3
"""
Audit theme DONE labels and remove them if the themed ad is missing.

For each theme:
1. Find all ad groups with {THEME}_DONE label
2. Check if ad group has an ad with the corresponding theme label
3. If themed ad is missing, remove the DONE label
"""

import sys
import argparse
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


def get_theme_labels(theme_name):
    """Get the theme label and done label for a given theme name."""
    return THEMES.get(theme_name, (None, None))


def audit_theme_done_labels(client, customer_id, theme_name, dry_run=True):
    """
    Audit DONE labels for a specific theme and customer.

    Returns:
        dict with stats about the audit
    """
    ga_service = client.get_service('GoogleAdsService')
    ad_group_label_service = client.get_service('AdGroupLabelService')

    theme_label, done_label_name = get_theme_labels(theme_name)

    if not theme_label or not done_label_name:
        print(f"[{customer_id}] Unknown theme: {theme_name} - skipping")
        return {
            'customer_id': customer_id,
            'theme': theme_name,
            'ad_groups_checked': 0,
            'ad_groups_with_done_label': 0,
            'ad_groups_missing_theme_ad': 0,
            'done_labels_removed': 0,
            'errors': 1
        }

    stats = {
        'customer_id': customer_id,
        'theme': theme_name,
        'ad_groups_checked': 0,
        'ad_groups_with_done_label': 0,
        'ad_groups_missing_theme_ad': 0,
        'done_labels_removed': 0,
        'errors': 0
    }

    print(f"\n[{customer_id}] Auditing theme: {theme_name} (label: {theme_label})")

    try:
        # Step 1: Find the DONE label resource
        label_query = f"""
            SELECT label.resource_name, label.name
            FROM label
            WHERE label.name = '{done_label_name}'
            LIMIT 1
        """

        label_response = ga_service.search(customer_id=customer_id, query=label_query)
        done_label_resource = None

        for row in label_response:
            done_label_resource = row.label.resource_name
            break

        if not done_label_resource:
            print(f"[{customer_id}] No {done_label_name} label found - skipping")
            return stats

        print(f"[{customer_id}] Found {done_label_name} label: {done_label_resource}")

        # Step 2: Find all ad groups with the DONE label
        ag_query = f"""
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group.resource_name,
                ad_group_label.resource_name
            FROM ad_group_label
            WHERE ad_group_label.label = '{done_label_resource}'
        """

        ag_response = ga_service.search(customer_id=customer_id, query=ag_query)
        ad_groups_to_check = []

        for row in ag_response:
            ad_groups_to_check.append({
                'id': str(row.ad_group.id),
                'name': row.ad_group.name,
                'resource': row.ad_group.resource_name,
                'label_resource': row.ad_group_label.resource_name
            })

        stats['ad_groups_with_done_label'] = len(ad_groups_to_check)
        print(f"[{customer_id}] Found {len(ad_groups_to_check)} ad groups with {done_label_name} label")

        if not ad_groups_to_check:
            return stats

        # Step 3: Batch query all ads in these ad groups
        ag_resources = "', '".join([ag['resource'] for ag in ad_groups_to_check])

        ads_query = f"""
            SELECT
                ad_group_ad.ad_group,
                ad_group_ad.resource_name
            FROM ad_group_ad
            WHERE ad_group_ad.ad_group IN ('{ag_resources}')
            AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
            AND ad_group_ad.status != REMOVED
        """

        ads_response = ga_service.search(customer_id=customer_id, query=ads_query)

        # Collect all ads by ad group
        ad_group_ads = {}
        all_ad_resources = []
        for row in ads_response:
            ag_res = row.ad_group_ad.ad_group
            ad_res = row.ad_group_ad.resource_name

            if ag_res not in ad_group_ads:
                ad_group_ads[ag_res] = []
            ad_group_ads[ag_res].append(ad_res)
            all_ad_resources.append(ad_res)

        # Step 4: Query labels for all ads
        ad_labels_map = {}  # ad_resource -> set of labels
        if all_ad_resources:
            # Query in batches of 1000 to avoid query size limits
            batch_size = 1000
            for i in range(0, len(all_ad_resources), batch_size):
                batch_ads = all_ad_resources[i:i+batch_size]
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
                    print(f"[{customer_id}] Warning: Could not query labels for batch {i//batch_size}: {e}")

        # Build map: ad_group_resource -> has theme ad
        ad_group_has_theme_ad = {}
        for ag_res, ads in ad_group_ads.items():
            ad_group_has_theme_ad[ag_res] = any(
                theme_label in ad_labels_map.get(ad_res, set())
                for ad_res in ads
            )

        # Step 5: Check each ad group and remove DONE label if theme ad is missing
        operations = []

        for ag in ad_groups_to_check:
            stats['ad_groups_checked'] += 1
            ag_resource = ag['resource']

            # Check if ad group has the theme ad
            has_theme_ad = ad_group_has_theme_ad.get(ag_resource, False)

            if not has_theme_ad:
                stats['ad_groups_missing_theme_ad'] += 1
                print(f"[{customer_id}] ⚠️  Ad group {ag['id']} ({ag['name']}) has {done_label_name} but NO {theme_label} ad")

                if not dry_run:
                    # Remove the DONE label
                    operation = client.get_type('AdGroupLabelOperation')
                    operation.remove = ag['label_resource']
                    operations.append(operation)

        # Execute removals
        if operations and not dry_run:
            try:
                response = ad_group_label_service.mutate_ad_group_labels(
                    customer_id=customer_id,
                    operations=operations
                )
                stats['done_labels_removed'] = len(response.results)
                print(f"[{customer_id}] ✅ Removed {len(response.results)} {done_label_name} labels")
            except GoogleAdsException as e:
                print(f"[{customer_id}] ❌ Error removing labels: {e}")
                stats['errors'] += 1
        elif operations and dry_run:
            print(f"[{customer_id}] 🔍 DRY RUN: Would remove {len(operations)} {done_label_name} labels")

    except Exception as e:
        print(f"[{customer_id}] ❌ Error auditing {theme_name}: {e}")
        stats['errors'] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description='Audit theme DONE labels')
    parser.add_argument('--execute', action='store_true', help='Actually remove labels (default is dry-run)')
    parser.add_argument('--parallel', type=int, default=5, help='Number of parallel workers')
    parser.add_argument('--themes', nargs='+', help='Specific themes to audit (default: all)')
    parser.add_argument('--customer', help='Specific customer ID to audit (default: all)')
    args = parser.parse_args()

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
    print(f"Parallel workers: {args.parallel}\n")

    # Create tasks
    tasks = []
    for customer_id in customers:
        for theme in themes_to_audit:
            tasks.append((customer_id, theme, dry_run))

    # Run audits in parallel
    all_stats = []

    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        futures = {
            executor.submit(audit_theme_done_labels, client, customer_id, theme, dry_run): (customer_id, theme)
            for customer_id, theme, dry_run in tasks
        }

        for future in as_completed(futures):
            customer_id, theme = futures[future]
            try:
                stats = future.result()
                all_stats.append(stats)
            except Exception as e:
                print(f"❌ Error processing {customer_id}/{theme}: {e}")

    # Print summary
    print("\n" + "=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)

    total_checked = sum(s['ad_groups_checked'] for s in all_stats)
    total_with_done = sum(s['ad_groups_with_done_label'] for s in all_stats)
    total_missing = sum(s['ad_groups_missing_theme_ad'] for s in all_stats)
    total_removed = sum(s['done_labels_removed'] for s in all_stats)
    total_errors = sum(s['errors'] for s in all_stats)

    print(f"Ad groups checked: {total_checked}")
    print(f"Ad groups with DONE labels: {total_with_done}")
    print(f"Ad groups missing theme ad: {total_missing}")

    if dry_run:
        print(f"DONE labels to remove: {total_missing}")
    else:
        print(f"DONE labels removed: {total_removed}")

    print(f"Errors: {total_errors}")

    # Print details by theme
    print("\nBy theme:")
    for theme in themes_to_audit:
        theme_stats = [s for s in all_stats if s['theme'] == theme]
        if theme_stats:
            missing = sum(s['ad_groups_missing_theme_ad'] for s in theme_stats)
            total = sum(s['ad_groups_with_done_label'] for s in theme_stats)
            print(f"  {theme:15s}: {missing:4d} missing / {total:4d} with DONE label")

    print("\n✅ Audit complete!")

    if dry_run and total_missing > 0:
        print(f"\n💡 Run with --execute to remove {total_missing} invalid DONE labels")


if __name__ == '__main__':
    main()
