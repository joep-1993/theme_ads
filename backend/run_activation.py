#!/usr/bin/env python3
"""Direct activation runner - bypasses HTTP endpoint"""
import asyncio
import sys
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "backend"))
sys.path.insert(0, str(Path(__file__).parent / "thema_ads_optimized"))

async def main():
    print("🚀 Starting V2 activation for 15 customers...")

    # Import after path setup
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / "thema_ads_optimized" / ".env"
    load_dotenv(env_path)

    from config import load_config_from_env
    from google_ads_client import initialize_client
    from backend.thema_ads_service import thema_ads_service

    print("📡 Initializing Google Ads client...")
    config = load_config_from_env()
    client = initialize_client(config.google_ads)

    print("✅ Client initialized")
    print("🎯 Running activation (parallel_workers=5)...")

    # Run activation
    result = await thema_ads_service.activate_ads_per_plan_v2(
        client=client,
        customer_ids=None,  # Use all from plan
        parallel_workers=5,
        reset_labels=False
    )

    print("\n" + "="*60)
    print("📊 ACTIVATION RESULTS:")
    print("="*60)
    print(f"Status: {result.get('status', 'unknown')}")

    stats = result.get('stats', {})
    print(f"\n✅ Customers processed: {stats.get('customers_processed', 0)}")
    print(f"❌ Customers failed: {stats.get('customers_failed', 0)}")
    print(f"📦 Ad groups activated: {stats.get('ad_groups_activated', 0)}")
    print(f"🟢 Theme ads enabled: {stats.get('theme_ads_enabled', 0)}")
    print(f"⏸️  Other theme ads paused: {stats.get('other_theme_ads_paused', 0)}")

    errors = stats.get('errors', [])
    if errors:
        print(f"\n⚠️  Errors ({len(errors)}):")
        for error in errors[:10]:  # Show first 10
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")

    print("="*60)
    return result

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result.get('status') == 'completed' else 1)
