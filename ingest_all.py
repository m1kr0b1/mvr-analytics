#!/usr/bin/env python3
"""
MVR Bulletins Bulk Ingestion Script
Paginated scraper that processes all bulletins since 1.1.2025

Usage:
    python ingest_all.py              # Start from beginning (or resume if interrupted)
    python ingest_all.py --resume     # Resume from last checkpoint
    python ingest_all.py --force       # Reprocess all (ignore checkpoint)
    python ingest_all.py --status     # Show current status
"""
import argparse
import asyncio
import sys
import os
import json
import time
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urljoin

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx
from bs4 import BeautifulSoup

from config import get_settings
from database import init_database, get_db_manager, Bulletin, CrimeIncident
from scraper import MVRScraper, get_bulletins_async
from extractor import CrimeIncidentExtractor


# Configuration
START_DATE = date(2025, 1, 1)
BASE_URL = "https://mvr.gov.mk/mk-MK/odnosi-so-javnost/dnevni-bilteni"
PAGES = range(1, 53)  # Pages 1-52
PAGE_DELAY = 30  # Seconds between pages
CHECKPOINT_FILE = Path(__file__).parent / ".ingest_checkpoint.json"


class CheckpointManager:
    """Manage checkpoint for resume functionality."""
    
    def __init__(self):
        self.data = self._load()
    
    def _load(self):
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE) as f:
                    return json.load(f)
            except:
                pass
        return {
            "last_page": 0,
            "processed_urls": [],
            "total_incidents": 0,
            "start_time": None,
            "last_update": None
        }
    
    def save(self):
        self.data["last_update"] = datetime.now().isoformat()
        try:
            with open(CHECKPOINT_FILE, "w") as f:
                json.dump(self.data, f, indent=2)
            print(f"    [CHECKPOINT] Saved: page={self.data['last_page']}, urls={len(self.data['processed_urls'])}")
        except Exception as e:
            print(f"    [CHECKPOINT ERROR] {e}")
    
    def set_last_page(self, page):
        self.data["last_page"] = page
        self.save()
    
    def add_processed_url(self, url):
        if url not in self.data["processed_urls"]:
            self.data["processed_urls"].append(url)
        self.save()
    
    def add_incidents(self, count):
        self.data["total_incidents"] += count
        self.save()
    
    def reset(self):
        self.data = {
            "last_page": 0,
            "processed_urls": [],
            "total_incidents": 0,
            "start_time": datetime.now().isoformat(),
            "last_update": None
        }
        self.save()


async def fetch_page_bulletins(page: int) -> list:
    """Fetch all bulletin links from a specific page."""
    url = f"{BASE_URL}?page={page}"
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching page {page}/52...")
    
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        bulletins = []
        
        # Find all bulletin links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/izvadok-na-del-od-dnevnite-nastani-" in href:
                full_url = urljoin("https://mvr.gov.mk", href)
                
                # Parse date from URL
                import re
                match = re.search(r"izvadok-na-del-od-dnevnite-nastani-(\d{8})", href)
                if match:
                    date_str = match.group(1)
                    day = int(date_str[:2])
                    month = int(date_str[2:4])
                    year = int(date_str[4:8])
                    pub_date = date(year, month, day)
                    
                    bulletins.append({
                        "url": full_url,
                        "date": pub_date
                    })
        
        # Remove duplicates
        seen = set()
        unique = []
        for b in bulletins:
            if b["url"] not in seen:
                seen.add(b["url"])
                unique.append(b)
        
        return unique


async def process_bulletin(db_manager, bulletin: dict, force: bool = False) -> int:
    """Process a single bulletin."""
    url = bulletin["url"]
    pub_date = bulletin["date"]
    
    # Check if already processed
    if not force:
        with db_manager.get_session() as session:
            existing = session.query(Bulletin).filter(Bulletin.url == url).first()
            if existing and existing.status == "PROCESSED":
                return 0  # Already done
    
    scraper = MVRScraper()
    extractor = CrimeIncidentExtractor()
    
    try:
        # Fetch content
        print(f"    [FETCH] {pub_date}")
        _, _, raw_text = await scraper.fetch_bulletin_content(url)
        
        # Save to DB
        with db_manager.get_session() as session:
            bulletin_obj = session.query(Bulletin).filter(Bulletin.url == url).first()
            is_new = bulletin_obj is None
            
            if is_new:
                bulletin_obj = Bulletin(
                    url=url,
                    publication_date=pub_date,
                    raw_text=raw_text,
                    status="PENDING"
                )
                session.add(bulletin_obj)
                session.flush()
            else:
                bulletin_obj.raw_text = raw_text
                bulletin_obj.status = "PENDING"
            
            session.commit()
            
            # Extract incidents
            print(f"    [LLM] Extracting incidents...")
            incidents = await extractor.extract_incidents(raw_text)
            
            for inc in incidents:
                # Ensure proper types for database
                crime_date = inc.get("crime_date")
                if crime_date and not hasattr(crime_date, 'year'):
                    crime_date = None  # Invalid date format
                
                perpetrator_ages = inc.get("perpetrator_ages")
                if perpetrator_ages is None:
                    perpetrator_ages = []
                if not isinstance(perpetrator_ages, list):
                    perpetrator_ages = []
                
                crime_inc = CrimeIncident(
                    bulletin_id=bulletin_obj.id,
                    crime_type=inc.get("crime_type") or "unknown",
                    crime_date=crime_date,
                    location_city=inc.get("location_city") or "unknown",
                    location_address=inc.get("location_address"),
                    perpetrator_count=inc.get("perpetrator_count") or "unknown",
                    perpetrator_ages=perpetrator_ages,
                    perpetrator_gender=inc.get("perpetrator_gender") or "unknown",
                    outcome=inc.get("outcome"),
                    raw_text=str(inc.get("raw_text") or "")[:2000]  # Ensure not None, truncate
                )
                session.add(crime_inc)
            
            bulletin_obj.status = "PROCESSED"
            session.commit()
            
            print(f"    [DONE] {len(incidents)} incidents")
            return len(incidents)
    
    except Exception as e:
        print(f"    [ERROR] {type(e).__name__}: {e}")
        
        # Get raw LLM output if available
        raw_llm_output = getattr(e, 'raw_output', None) or getattr(e, 'raw_llm_output', None)
        
        # Also check extractor for last response
        if not raw_llm_output and hasattr(extractor, '_last_raw_output'):
            raw_llm_output = extractor._last_raw_output
        
        # Save error to database with raw data
        try:
            from database import ProcessingError
            with db_manager.get_session() as session:
                bulletin_obj = session.query(Bulletin).filter(Bulletin.url == url).first()
                error = ProcessingError(
                    bulletin_id=bulletin_obj.id if bulletin_obj else None,
                    error_type=type(e).__name__,
                    error_detail=str(e)[:1000],
                    raw_llm_output=raw_llm_output[:10000] if raw_llm_output else (raw_text[:10000] if 'raw_text' in dir() and raw_text else None)
                )
                session.add(error)
                session.commit()
                print(f"    [ERROR LOGGED] Saved to processing_errors table")
                if raw_llm_output:
                    print(f"    [RAW OUTPUT] {len(raw_llm_output)} chars saved")
        except Exception as log_err:
            print(f"    [ERROR LOG] Failed to save error: {log_err}")
        return 0
    
    finally:
        await scraper.close()
        await extractor.close()


async def run_ingestion(force: bool = False, resume: bool = False):
    """Main ingestion loop."""
    print("=" * 60)
    print("MVR BULLETINS BULK INGESTION")
    print("=" * 60)
    print(f"Start date: {START_DATE}")
    print(f"Pages: 1-52")
    print(f"Page delay: {PAGE_DELAY}s")
    print(f"Mode: {'FORCE REPROCESS' if force else 'Normal (skip existing)'}")
    print("=" * 60)
    
    db_manager = init_database()
    checkpoint = CheckpointManager()
    
    if force:
        checkpoint.reset()
        print("[INFO] Checkpoint reset (--force)")
    elif resume:
        print(f"[INFO] Resuming from page {checkpoint.data['last_page']}")
    
    start_page = checkpoint.data["last_page"] + 1 if resume else 1
    
    total_incidents = checkpoint.data["total_incidents"]
    total_bulletins = 0
    
    print(f"[START] Starting from page {start_page}")
    
    try:
        for page in PAGES:
            if page < start_page:
                continue
            
            print(f"\n{'='*60}")
            print(f"PAGE {page}/52")
            print(f"{'='*60}")
            
            # Fetch bulletins from page
            bulletins = await fetch_page_bulletins(page)
            print(f"Found {len(bulletins)} bulletins on page {page}")
            
            # Filter by date
            filtered = [b for b in bulletins if b["date"] >= START_DATE]
            print(f"After date filter (>= {START_DATE}): {len(filtered)} bulletins")
            
            # Process each bulletin
            page_incidents = 0
            page_bulletins = 0
            
            for bulletin in filtered:
                incidents = await process_bulletin(db_manager, bulletin, force)
                if incidents > 0:
                    page_incidents += incidents
                    page_bulletins += 1
                    total_incidents += incidents
                    checkpoint.add_incidents(incidents)
                
                checkpoint.add_processed_url(bulletin["url"])
            
            total_bulletins += page_bulletins
            
            # Update checkpoint
            checkpoint.set_last_page(page)
            
            print(f"\nPage {page} summary:")
            print(f"  Bulletins processed: {page_bulletins}")
            print(f"  Incidents extracted: {page_incidents}")
            print(f"  Total so far: {total_bulletins} bulletins, {total_incidents} incidents")
            
            # Wait before next page (except last)
            if page < 52:
                print(f"\n[WAIT] Sleeping {PAGE_DELAY}s before next page...")
                time.sleep(PAGE_DELAY)
    
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Saving checkpoint...")
        checkpoint.save()
        print(f"Resumable from page {checkpoint.data['last_page'] + 1}")
    
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    print(f"Total bulletins: {total_bulletins}")
    print(f"Total incidents: {total_incidents}")
    print(f"Resume from: page {checkpoint.data['last_page'] + 1}")


def show_status():
    """Show current ingestion status."""
    checkpoint = CheckpointManager()
    
    print("\n" + "=" * 60)
    print("INGESTION STATUS")
    print("=" * 60)
    print(f"Last page processed: {checkpoint.data['last_page']}/52")
    print(f"Total incidents extracted: {checkpoint.data['total_incidents']}")
    print(f"Processed URLs: {len(checkpoint.data['processed_urls'])}")
    
    if checkpoint.data.get("start_time"):
        print(f"Started: {checkpoint.data['start_time']}")
    if checkpoint.data.get("last_update"):
        print(f"Last update: {checkpoint.data['last_update']}")
    
    # Show DB stats
    db_manager = init_database()
    with db_manager.get_session() as session:
        from sqlalchemy import func
        total = session.query(func.count(Bulletin.id)).scalar()
        processed = session.query(func.count(Bulletin.id)).filter(Bulletin.status == "PROCESSED").scalar()
        incidents = session.query(func.count(CrimeIncident.id)).scalar()
        
        print(f"\nDatabase:")
        print(f"  Total bulletins: {total}")
        print(f"  Processed: {processed}")
        print(f"  Crime incidents: {incidents}")
    
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="MVR Bulletins Bulk Ingestion")
    parser.add_argument("--force", "-f", action="store_true", help="Force reprocess all (ignore checkpoint)")
    parser.add_argument("--resume", "-r", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--status", "-s", action="store_true", help="Show status and exit")
    parser.add_argument("--reset", action="store_true", help="Reset checkpoint file")
    
    args = parser.parse_args()
    
    if args.status:
        show_status()
        return
    
    if args.reset:
        checkpoint = CheckpointManager()
        checkpoint.reset()
        print("Checkpoint reset.")
        return
    
    force = args.force or args.resume  # If resuming, also reprocess skipped
    asyncio.run(run_ingestion(force=force, resume=args.resume))


if __name__ == "__main__":
    main()
