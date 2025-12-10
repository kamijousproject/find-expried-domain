#!/usr/bin/env python3
"""
main.py - Dead Website Finder CLI

CLI application สำหรับค้นหาธุรกิจจาก Google Maps และตรวจสอบว่าเว็บไซต์ยังทำงานอยู่หรือไม่
สำหรับใช้เป็น sales leads ในการขายบริการทำเว็บไซต์ให้ SME ไทย

Usage:
    # ค้นหาด้วย keyword และจังหวัด
    python main.py --keywords "ร้านอาหาร,คลินิก" --city "Bangkok"
    
    # ค้นหาด้วย bounding box
    python main.py --keywords "hotel" --bounds "13.5,100.3,13.9,100.9"
    
    # ใช้ค่าจาก .env file
    python main.py
    
    # Mock mode สำหรับทดสอบ (ไม่ใช้ API จริง)
    python main.py --mock

Author: Dead Website Finder Team
License: MIT
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from typing import List, Optional

from config import Config, load_config, SearchBounds
from models import Business, Lead, WebsiteCheckResult, WebsiteStatus
from google_maps_client import GoogleMapsClient
from website_checker import WebsiteChecker
from lead_filter import create_default_filter, create_quality_filter
from database import Database
from exporter import Exporter, create_sample_csv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("dead_website_finder.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Dead Website Finder - ค้นหาธุรกิจที่มีเว็บไซต์มีปัญหา",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # ค้นหาร้านอาหารในกรุงเทพ
  python main.py --keywords "ร้านอาหาร" --city "Bangkok"
  
  # ค้นหาหลาย keywords
  python main.py --keywords "restaurant,hotel,clinic" --city "Chiang Mai"
  
  # ใช้ bounding box
  python main.py --keywords "ร้านอาหาร" --bounds "13.5,100.3,13.9,100.9"
  
  # Mode ทดสอบ (mock data)
  python main.py --mock
  
  # เช็กเว็บไซต์เดียว
  python main.py --check-url "https://example.com"
        """
    )
    
    # Search options
    parser.add_argument(
        "--keywords", "-k",
        type=str,
        help="Keywords to search, comma-separated (e.g., 'restaurant,hotel,ร้านอาหาร')"
    )
    parser.add_argument(
        "--city", "-c",
        type=str,
        help="City or province name (e.g., 'Bangkok', 'เชียงใหม่', 'Phuket')"
    )
    parser.add_argument(
        "--bounds", "-b",
        type=str,
        help="Bounding box as 'south_lat,west_lng,north_lat,east_lng'"
    )
    parser.add_argument(
        "--radius", "-r",
        type=int,
        default=10000,
        help="Search radius in meters (default: 10000)"
    )
    
    # Website checker options
    parser.add_argument(
        "--concurrent", "-n",
        type=int,
        default=100,
        help="Number of concurrent connections for website checking (default: 100)"
    )
    parser.add_argument(
        "--timeout", "-t",
        type=int,
        default=10,
        help="Request timeout in seconds (default: 10)"
    )
    
    # Filter options
    parser.add_argument(
        "--min-rating",
        type=float,
        default=0.0,
        help="Minimum rating filter (default: 0.0)"
    )
    parser.add_argument(
        "--min-reviews",
        type=int,
        default=0,
        help="Minimum reviews filter (default: 0)"
    )
    parser.add_argument(
        "--require-phone",
        action="store_true",
        help="Only include leads with phone number"
    )
    parser.add_argument(
        "--quality-filter",
        action="store_true",
        help="Use quality filter (rating >= 3.5, reviews >= 5, require phone)"
    )
    
    # Output options
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="./output",
        help="Output directory (default: ./output)"
    )
    parser.add_argument(
        "--output-name",
        type=str,
        default="dead_websites_leads.csv",
        help="Output CSV filename (default: dead_websites_leads.csv)"
    )
    
    # Special modes
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Run in mock mode with sample data (no API calls)"
    )
    parser.add_argument(
        "--check-url",
        type=str,
        help="Check a single URL and exit"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous run (use existing database)"
    )
    parser.add_argument(
        "--skip-search",
        action="store_true",
        help="Skip search phase, only check websites from existing database"
    )
    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Only export data from existing database"
    )
    
    # Debug options
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode"
    )
    
    return parser.parse_args()


async def check_single_url(url: str, timeout: int = 10):
    """
    ตรวจสอบ URL เดียว
    
    Args:
        url: URL to check
        timeout: Request timeout
    """
    print(f"\nChecking URL: {url}")
    print("-" * 50)
    
    checker = WebsiteChecker(concurrent_limit=1, timeout=timeout)
    result = await checker.check_single(url)
    
    print(f"Status: {result.status.value}")
    print(f"Status Code: {result.status_code or 'N/A'}")
    print(f"Reason: {result.reason}")
    print(f"Response Time: {result.response_time_ms:.0f}ms" if result.response_time_ms else "Response Time: N/A")
    print(f"Final URL: {result.final_url or 'N/A'}")
    print(f"Is Dead: {'Yes' if result.is_dead() else 'No'}")
    print("-" * 50)


async def run_mock_mode(args):
    """
    รัน mock mode ด้วย sample data
    
    ใช้สำหรับทดสอบระบบโดยไม่ต้องใช้ Google API
    """
    print("\n" + "=" * 60)
    print("RUNNING IN MOCK MODE (No API calls)")
    print("=" * 60)
    
    # Create sample businesses
    mock_businesses = [
        Business(
            place_id="mock1",
            name="ร้านอาหาร สมชาย",
            formatted_address="123 ถนนสุขุมวิท กรุงเทพ",
            formatted_phone_number="02-123-4567",
            website="https://somchai-restaurant.com",
            rating=4.5,
            user_ratings_total=156,
        ),
        Business(
            place_id="mock2",
            name="คลินิกหมอสุดา",
            formatted_address="456 ถนนพระราม 4 กรุงเทพ",
            formatted_phone_number="02-987-6543",
            website="https://drsuda-clinic.co.th",
            rating=4.8,
            user_ratings_total=89,
        ),
        Business(
            place_id="mock3",
            name="อู่ซ่อมรถ วิชัย",
            formatted_address="789 ซอยลาดพร้าว 15 กรุงเทพ",
            formatted_phone_number="081-234-5678",
            website="https://vichai-garage.com",
            rating=4.2,
            user_ratings_total=45,
        ),
        Business(
            place_id="mock4",
            name="โรงแรม ริเวอร์ไซด์",
            formatted_address="111 ถนนช้างคลาน เชียงใหม่",
            formatted_phone_number="053-456-789",
            website="https://riverside-hotel-cm.com",
            rating=3.9,
            user_ratings_total=234,
        ),
        Business(
            place_id="mock5",
            name="ร้านนวดไทย สบาย",
            formatted_address="222 ถนนสีลม กรุงเทพ",
            formatted_phone_number="02-555-1234",
            website="https://sabai-thaimassage.net",
            rating=4.6,
            user_ratings_total=312,
        ),
        Business(
            place_id="mock6",
            name="ร้านกาแฟ บ้านสวน",
            formatted_address="444 ซอยอารีย์ กรุงเทพ",
            formatted_phone_number="086-999-0000",
            website="https://google.com",  # This one will be OK
            rating=4.7,
            user_ratings_total=567,
        ),
        Business(
            place_id="mock7",
            name="ร้านอาหารไม่มีเว็บ",
            formatted_address="555 ถนนพระราม 9 กรุงเทพ",
            formatted_phone_number="02-111-2222",
            website="",  # No website
            rating=4.0,
            user_ratings_total=100,
        ),
    ]
    
    print(f"\nMock businesses created: {len(mock_businesses)}")
    
    # Check websites
    print("\n--- Checking Websites ---")
    checker = WebsiteChecker(
        concurrent_limit=args.concurrent,
        timeout=args.timeout
    )
    
    # Get URLs to check
    urls_to_check = [(b.place_id, b.website) for b in mock_businesses if b.website]
    
    print(f"Websites to check: {len(urls_to_check)}")
    
    # Check websites
    results = await checker.check_many([url for _, url in urls_to_check])
    
    # Map results back to businesses
    url_to_result = {r.url: r for r in results}
    
    for business in mock_businesses:
        if business.website:
            normalized_url = checker._normalize_url(business.website)
            if normalized_url in url_to_result:
                business.website_check_result = url_to_result[normalized_url]
    
    # Filter leads
    print("\n--- Filtering Leads ---")
    if args.quality_filter:
        lead_filter = create_quality_filter()
    else:
        lead_filter = create_default_filter()
    
    leads = lead_filter.filter_leads(mock_businesses)
    
    print(f"Leads found: {len(leads)}")
    
    # Export
    print("\n--- Exporting Results ---")
    exporter = Exporter(args.output)
    
    csv_path = exporter.export_leads_csv(leads, args.output_name)
    print(f"Leads CSV: {csv_path}")
    
    all_csv_path = exporter.export_all_businesses_csv(mock_businesses, "all_businesses.csv")
    print(f"All businesses CSV: {all_csv_path}")
    
    # Print summary
    search_info = {
        "keywords": args.keywords or "mock",
        "city": args.city or "mock",
        "mode": "mock"
    }
    report = exporter.generate_summary_report(mock_businesses, leads, search_info)
    print(report)
    
    # Save summary
    summary_path = exporter.save_summary_report(mock_businesses, leads, "summary_report.txt", search_info)
    print(f"\nSummary saved to: {summary_path}")


async def run_full_pipeline(args):
    """
    รัน pipeline เต็มรูปแบบ
    
    1. Load config
    2. Search businesses from Google Maps
    3. Check websites
    4. Filter leads
    5. Export results
    """
    print("\n" + "=" * 60)
    print("DEAD WEBSITE FINDER - Starting Full Pipeline")
    print("=" * 60)
    
    # Load configuration
    try:
        keywords = args.keywords.split(",") if args.keywords else None
        config = load_config(
            keywords=keywords,
            bounds=args.bounds,
            city=args.city,
            concurrent=args.concurrent,
            timeout=args.timeout,
            output_dir=args.output,
        )
    except ValueError as e:
        print(f"\nConfiguration Error: {e}")
        print("Please create a .env file with GOOGLE_MAPS_API_KEY")
        print("See .env.example for reference")
        sys.exit(1)
    
    # Update config from args
    config.search_radius = args.radius
    config.output_filename = args.output_name
    
    # Initialize database
    db = Database(config.db_path)
    
    # Initialize exporter
    exporter = Exporter(config.output_dir)
    
    # Search businesses (unless skip-search or export-only)
    if not args.skip_search and not args.export_only:
        print(f"\n--- Searching Businesses ---")
        print(f"Keywords: {config.keywords}")
        print(f"City: {config.city or 'N/A'}")
        print(f"Bounds: {config.search_bounds or 'N/A'}")
        
        async with GoogleMapsClient(config) as client:
            total_found = 0
            
            async for business in client.search_all_keywords(
                keywords=config.keywords,
                city=config.city,
                bounds=config.search_bounds,
            ):
                db.insert_business(business)
                total_found += 1
                
                if total_found % 10 == 0:
                    print(f"  Found {total_found} businesses...")
        
        print(f"Total businesses found: {total_found}")
    
    # Get businesses to check
    if args.resume or args.skip_search:
        businesses = db.get_unchecked_websites()
        print(f"\nResuming: {len(businesses)} websites to check")
    elif args.export_only:
        businesses = list(db.get_all_businesses())
        print(f"\nExport only: {len(businesses)} businesses in database")
    else:
        businesses = db.get_businesses_with_website()
        print(f"\nBusinesses with website: {len(businesses)}")
    
    # Check websites (unless export-only)
    if not args.export_only:
        businesses_to_check = [b for b in businesses if b.website and not b.website_check_result]
        
        if businesses_to_check:
            print(f"\n--- Checking {len(businesses_to_check)} Websites ---")
            
            checker = WebsiteChecker(
                concurrent_limit=config.concurrent_requests,
                timeout=config.request_timeout
            )
            
            urls_to_check = [b.website for b in businesses_to_check]
            results = await checker.check_many(urls_to_check)
            
            # Map results back and update database
            url_to_result = {}
            for result in results:
                url_to_result[result.url] = result
                # Also map normalized version
                url_to_result[checker._normalize_url(result.url)] = result
            
            for business in businesses_to_check:
                normalized = checker._normalize_url(business.website)
                if normalized in url_to_result:
                    result = url_to_result[normalized]
                    business.website_check_result = result
                    db.update_website_check(business.place_id, result)
            
            print(f"Websites checked: {len(results)}")
            print(f"Dead websites: {checker.total_dead}")
    
    # Get all businesses from database for filtering
    all_businesses = list(db.get_all_businesses())
    
    # Filter leads
    print("\n--- Filtering Leads ---")
    if args.quality_filter:
        lead_filter = create_quality_filter()
        print("Using quality filter")
    else:
        lead_filter = create_default_filter()
        print("Using default filter")
    
    leads = lead_filter.filter_leads(all_businesses)
    print(f"Leads found: {len(leads)}")
    
    # Export results
    print("\n--- Exporting Results ---")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Main leads CSV
    csv_path = exporter.export_leads_csv(leads, config.output_filename)
    print(f"Leads CSV: {csv_path}")
    
    # All businesses CSV
    all_csv = exporter.export_all_businesses_csv(all_businesses, f"all_businesses_{timestamp}.csv")
    print(f"All businesses CSV: {all_csv}")
    
    # JSON exports
    json_path = exporter.export_leads_json(leads, f"leads_{timestamp}.json")
    print(f"Leads JSON: {json_path}")
    
    # Summary report
    search_info = {
        "keywords": ",".join(config.keywords),
        "city": config.city or "N/A",
        "bounds": str(config.search_bounds) if config.search_bounds else "N/A",
    }
    
    report = exporter.generate_summary_report(all_businesses, leads, search_info)
    print(report)
    
    summary_path = exporter.save_summary_report(
        all_businesses, leads, f"summary_{timestamp}.txt", search_info
    )
    print(f"\nSummary saved to: {summary_path}")
    
    # Database stats
    print("\n--- Database Statistics ---")
    stats = db.get_statistics()
    print(f"Total businesses: {stats['total_businesses']}")
    print(f"With website: {stats['with_website']}")
    print(f"Websites checked: {stats['websites_checked']}")
    print(f"OK websites: {stats['websites_ok']}")
    print(f"Dead websites: {stats['websites_dead']}")
    
    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)


async def main():
    """Main entry point"""
    args = parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.verbose:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Handle special modes
    if args.check_url:
        await check_single_url(args.check_url, args.timeout)
        return
    
    if args.mock:
        await run_mock_mode(args)
        return
    
    # Run full pipeline
    await run_full_pipeline(args)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error")
        print(f"\nError: {e}")
        sys.exit(1)
