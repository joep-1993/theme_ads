"""
Check labels on specific ad group to see if it has SD_DONE or theme labels.
"""
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment
env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
load_dotenv(env_path)

# Add paths
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))

from config import load_config_from_env
from google_ads_client import initialize_client

config = load_config_from_env()
client = initialize_client(config.google_ads)
ga_service = client.get_service('GoogleAdsService')

customer_id = '8338942127'
ad_group_id = '155636546387'

# Get all labels on this ad group
query = f'''
    SELECT
        ad_group.id,
        ad_group.name,
        label.name
    FROM ad_group_label
    WHERE ad_group.id = {ad_group_id}
'''

print(f'Checking labels on ad group {ad_group_id}...\n')
response = ga_service.search(customer_id=customer_id, query=query)

labels_found = []
for row in response:
    labels_found.append(row.label.name)
    print(f'  ✓ Label: {row.label.name}')

print()
print('='*80)
print('ANALYSIS')
print('='*80)

if not labels_found:
    print('❌ NO LABELS FOUND on this ad group!')
    print()
    print('This ad group does NOT have SD_DONE or any theme label.')
    print('It should have been discovered and processed by "Run all themes"!')
    print()
    print('Possible reasons it was skipped:')
    print('  1. Ad group was created AFTER discovery ran')
    print('  2. Campaign was excluded from discovery')
    print('  3. Ad group had no active ads when discovery ran')
else:
    print(f'Found {len(labels_found)} label(s) on this ad group:')

    if 'SD_DONE' in labels_found:
        print('  ✓ Has SD_DONE label - would be SKIPPED by auto-discovery')
        print('    (This is expected behavior - SD_DONE prevents reprocessing)')
    else:
        print('  ✗ Does NOT have SD_DONE label - should have been discovered!')

    theme_labels = [l for l in labels_found if l.startswith('THEME_')]
    if theme_labels:
        print(f'  ✓ Has theme labels: {theme_labels}')
        print(f'    (Only {len(theme_labels)} theme(s) instead of 5 expected)')

print('='*80)
