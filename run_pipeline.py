#!/usr/bin/env python3
"""
MVR Crime Bulletin Pipeline Runner
Run with: python run_pipeline.py [--url URL] [--force] [--verbose]

Options:
    --url URL     Process a specific bulletin URL
    --force      Reprocess even if already processed
    --verbose    Show detailed logging
    --stats      Show database statistics and exit
"""
import argparse
import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_settings, setup_logging
from database import init_database, get_db_manager, Bulletin, CrimeIncident, ProcessingError
from scraper import MVRScraper, get_bulletins_async
from extractor import CrimeIncidentExtractor
from sqlalchemy import text


async def process_bulletin(db_manager, url, pub_date, force_reprocess=False):
    """Process a single bulletin URL."""
    scraper = MVRScraper()
    extractor = CrimeIncidentExtractor()
    
    try:
        # Fetch content
        print(f"[FETCH] {url}")
        _, _, raw_text = await scraper.fetch_bulletin_content(url)
        
        # Save to database
        with db_manager.get_session() as session:
            bulletin = session.query(Bulletin).filter(Bulletin.url == url).first()
            is_new = bulletin is None
            
            if is_new:
                bulletin = Bulletin(
                    url=url,
                    publication_date=pub_date,
                    raw_text=raw_text,
                    status='PENDING',
                )
                session.add(bulletin)
                session.flush()
            else:
                bulletin.raw_text = raw_text
                bulletin.status = 'PENDING'
            
            session.commit()
            
            # Extract incidents
            print(f"[EXTRACT] Sending to LLM...")
            incidents = await extractor.extract_incidents(raw_text)
            
            for inc in incidents:
                crime_incident = CrimeIncident(
                    bulletin_id=bulletin.id,
                    crime_type=inc['crime_type'],
                    crime_date=inc.get('crime_date'),
                    location_city=inc['location_city'],
                    location_address=inc.get('location_address'),
                    perpetrator_count=inc['perpetrator_count'],
                    perpetrator_ages=inc.get('perpetrator_ages', []),
                    perpetrator_gender=inc['perpetrator_gender'],
                    outcome=inc.get('outcome'),
                    raw_text=inc['raw_text'],
                )
                session.add(crime_incident)
            
            bulletin.status = 'PROCESSED'
            session.commit()
            
            print(f"[DONE] Extracted {len(incidents)} incidents")
            return len(incidents)
    
    except Exception as e:
        print(f"[ERROR] {e}")
        with db_manager.get_session() as session:
            error = ProcessingError(
                bulletin_id=None,
                error_type=type(e).__name__,
                error_detail=str(e),
            )
            session.add(error)
            session.commit()
        return 0
    
    finally:
        await scraper.close()
        await extractor.close()


async def run_pipeline(force_reprocess=False, specific_url=None):
    """Run the full pipeline."""
    setup_logging()
    settings = get_settings()
    
    db_manager = init_database()
    
    if specific_url:
        # Process single URL
        print(f"[SINGLE] Processing: {specific_url}")
        scraper = MVRScraper()
        pub_date = scraper._parse_bulletin_date_from_url(specific_url)
        await process_bulletin(db_manager, specific_url, pub_date, force_reprocess)
    else:
        # Process all bulletins from index
        print("[FULL] Fetching bulletin index...")
        bulletins = await get_bulletins_async()
        
        if not bulletins:
            print("[WARN] No bulletins found")
            return
        
        print(f"[INDEX] Found {len(bulletins)} bulletins")
        
        total_incidents = 0
        processed = 0
        skipped = 0
        errors = 0
        
        for url, pub_date in bulletins:
            with db_manager.get_session() as session:
                bulletin = session.query(Bulletin).filter(Bulletin.url == url).first()
                if bulletin and bulletin.status == 'PROCESSED' and not force_reprocess:
                    print(f"[SKIP] Already processed: {url}")
                    skipped += 1
                    continue
            
            incidents = await process_bulletin(db_manager, url, pub_date, force_reprocess)
            if incidents > 0:
                processed += 1
                total_incidents += incidents
            else:
                errors += 1
        
        print("\n" + "="*50)
        print("PIPELINE COMPLETE")
        print("="*50)
        print(f"  Bulletins found: {len(bulletins)}")
        print(f"  Newly processed: {processed}")
        print(f"  Skipped: {skipped}")
        print(f"  Errors: {errors}")
        print(f"  Total incidents: {total_incidents}")


def show_stats():
    """Show database statistics."""
    db_manager = init_database()
    
    with db_manager.get_session() as session:
        bulletins = session.query(Bulletin).all()
        incidents = session.query(CrimeIncident).all()
        errors = session.query(ProcessingError).all()
        
        processed = sum(1 for b in bulletins if b.status == 'PROCESSED')
        pending = sum(1 for b in bulletins if b.status == 'PENDING')
        error = sum(1 for b in bulletins if b.status == 'ERROR')
        
        print("\n" + "="*50)
        print("DATABASE STATISTICS")
        print("="*50)
        print(f"  Bulletins: {len(bulletins)}")
        print(f"    - Processed: {processed}")
        print(f"    - Pending: {pending}")
        print(f"    - Error: {error}")
        print(f"  Crime Incidents: {len(incidents)}")
        print(f"  Processing Errors: {len(errors)}")
        
        if incidents:
            # City breakdown
            print("\n  Incidents by City:")
            city_counts = {}
            for inc in incidents:
                city = inc.location_city or 'Unknown'
                city_counts[city] = city_counts.get(city, 0) + 1
            for city, count in sorted(city_counts.items(), key=lambda x: -x[1])[:10]:
                print(f"    - {city}: {count}")
        
        print()


def main():
    parser = argparse.ArgumentParser(description='MVR Crime Bulletin Pipeline')
    parser.add_argument('--url', '-u', help='Process a specific bulletin URL')
    parser.add_argument('--force', '-f', action='store_true', help='Force reprocess')
    parser.add_argument('--stats', '-s', action='store_true', help='Show stats only')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.stats:
        show_stats()
        return
    
    asyncio.run(run_pipeline(force_reprocess=args.force, specific_url=args.url))


if __name__ == "__main__":
    main()
