"""
Pipeline orchestration for MVR Crime Bulletin scraper.
Coordinates scraping, extraction, and database persistence.
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime, date
from typing import Optional, List, Tuple
from dataclasses import dataclass, field

from sqlalchemy.exc import IntegrityError

from config import get_settings, setup_logging
from database import (
    init_database,
    get_db_manager,
    DatabaseManager,
    Bulletin,
    CrimeIncident,
    ProcessingError,
    BulletinStatus,
)
from scraper import MVRScraper, ScraperError, get_bulletins_async
from extractor import CrimeIncidentExtractor, JSONParseError, LLMAPIError
from geocoder import geocode_location

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    """Statistics from a pipeline run."""
    bulletins_checked: int = 0
    bulletins_new: int = 0
    bulletins_skipped: int = 0
    bulletins_errors: int = 0
    incidents_extracted: int = 0
    errors_encountered: int = 0

    def __str__(self) -> str:
        return (
            f"Pipeline Summary:\n"
            f"  - Bulletins checked: {self.bulletins_checked}\n"
            f"  - Bulletins newly processed: {self.bulletins_new}\n"
            f"  - Bulletins skipped (already processed): {self.bulletins_skipped}\n"
            f"  - Bulletins with errors: {self.bulletins_errors}\n"
            f"  - Crime incidents extracted: {self.incidents_extracted}\n"
            f"  - Total errors: {self.errors_encountered}"
        )


class MVRCrimeBulletinPipeline:
    """
    Main pipeline for scraping, extracting, and storing MVR crime bulletins.
    Implements idempotent processing - re-runs do not create duplicates.
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db_manager = db_manager or get_db_manager()
        self.scraper = MVRScraper()
        self.extractor = CrimeIncidentExtractor()
        self.stats = PipelineStats()
        self._shutdown = False

    async def close(self):
        """Clean up resources."""
        await self.scraper.close()
        await self.extractor.close()

    def _is_bulletin_processed(self, session, url: str) -> bool:
        """Check if a bulletin URL has already been processed."""
        existing = session.query(Bulletin).filter(Bulletin.url == url).first()
        return existing is not None

    def _get_bulletin_status(self, session, url: str) -> Optional[BulletinStatus]:
        """Get the status of a bulletin if it exists."""
        existing = session.query(Bulletin).filter(Bulletin.url == url).first()
        return existing.status if existing else None

    async def _process_single_bulletin(
        self,
        session,
        url: str,
        pub_date: date,
    ) -> int:
        """
        Process a single bulletin: fetch, extract, and store.

        Returns:
            Number of incidents extracted from this bulletin
        """
        print(f"STEP|FETCH|{pub_date}|{url}")  # Progress: Fetching bulletin
        logger.info(f"Processing bulletin: {url}")

        try:
            # Fetch the bulletin content
            print(f"STEP|EXTRACT|{pub_date}|Sending to LLM...")
            _, _, raw_text = await self.scraper.fetch_bulletin_content(url)

            # Create or update bulletin record
            bulletin = session.query(Bulletin).filter(Bulletin.url == url).first()
            is_new = bulletin is None

            if is_new:
                bulletin = Bulletin(
                    url=url,
                    publication_date=pub_date,
                    raw_text=raw_text,
                    status=BulletinStatus.PENDING,
                )
                session.add(bulletin)
                session.flush()  # Get the ID

            # Update existing bulletin
            bulletin.raw_text = raw_text
            bulletin.status = BulletinStatus.PENDING
            session.commit()

            # Extract crime incidents
            try:
                print(f"STEP|PARSE|{pub_date}|Parsing {len(raw_text)} chars with LLM...")
                incidents = await self.extractor.extract_incidents(raw_text)
                print(f"STEP|SAVED|{pub_date}|Extracted {len(incidents)} incidents")

                # Store incidents
                for incident_data in incidents:
                    # Try to geocode precise location
                    precise_lat = None
                    precise_lon = None
                    address = incident_data.get("location_address")
                    if address:
                        coords = geocode_location(
                            incident_data["location_city"],
                            address
                        )
                        if coords:
                            precise_lat, precise_lon = coords
                    
                    crime_incident = CrimeIncident(
                        bulletin_id=bulletin.id,
                        crime_type=incident_data["crime_type"],
                        crime_date=incident_data.get("crime_date"),
                        location_city=incident_data["location_city"],
                        location_address=address,
                        precise_lat=precise_lat,
                        precise_lon=precise_lon,
                        perpetrator_count=incident_data["perpetrator_count"],
                        perpetrator_ages=incident_data.get("perpetrator_ages", []),
                        perpetrator_gender=incident_data["perpetrator_gender"],
                        outcome=incident_data.get("outcome"),
                        raw_text=incident_data["raw_text"],
                    )
                    session.add(crime_incident)

                # Mark bulletin as processed
                bulletin.status = BulletinStatus.PROCESSED
                bulletin.processed_at = datetime.utcnow()
                session.commit()

                logger.info(f"Extracted {len(incidents)} incidents from {url}")
                return len(incidents)

            except JSONParseError as e:
                # Store error with raw output for manual review
                error = ProcessingError(
                    bulletin_id=bulletin.id,
                    error_type="JSONParseError",
                    error_detail=str(e),
                    raw_llm_output=raw_text[:10000] if len(raw_text) > 10000 else raw_text,
                )
                session.add(error)
                bulletin.status = BulletinStatus.ERROR
                session.commit()
                logger.error(f"JSON parse error for {url}: {e}")
                self.stats.errors_encountered += 1
                return 0

            except LLMAPIError as e:
                error = ProcessingError(
                    bulletin_id=bulletin.id,
                    error_type="LLMAPIError",
                    error_detail=str(e),
                )
                session.add(error)
                bulletin.status = BulletinStatus.ERROR
                session.commit()
                logger.error(f"LLM API error for {url}: {e}")
                self.stats.errors_encountered += 1
                return 0

        except ScraperError as e:
            # Store fetch error
            session.rollback()
            error = ProcessingError(
                bulletin_id=None,
                error_type="ScraperError",
                error_detail=str(e),
            )
            session.add(error)
            session.commit()
            logger.error(f"Scraper error for {url}: {e}")
            self.stats.errors_encountered += 1
            self.stats.bulletins_errors += 1
            return 0

    async def run(self, force_reprocess: bool = False) -> PipelineStats:
        """
        Run the complete pipeline.

        Args:
            force_reprocess: If True, re-process bulletins even if already processed

        Returns:
            PipelineStats with run statistics
        """
        logger.info("Starting MVR Crime Bulletin Pipeline")
        self.stats = PipelineStats()

        try:
            # Step 1: Get all bulletin links from index
            print("STEP|START|Fetching bulletin index from MVR...")
            bulletins = await get_bulletins_async()
            self.stats.bulletins_checked = len(bulletins)
            print(f"STEP|INDEX|Found {len(bulletins)} bulletins")

            if not bulletins:
                logger.warning("No bulletins found on index page")
                print("STEP|ERROR|No bulletins found on index page")
                return self.stats

            # Step 2: Process each bulletin
            with self.db_manager.get_session() as session:
                for i, (url, pub_date) in enumerate(bulletins):
                    if self._shutdown:
                        logger.info("Shutdown requested, stopping pipeline")
                        break

                    # Check if already processed
                    status = self._get_bulletin_status(session, url)

                    if status == BulletinStatus.PROCESSED and not force_reprocess:
                        print(f"STEP|SKIP|{pub_date}|Already processed - skipping")
                        self.stats.bulletins_skipped += 1
                        continue

                    print(f"STEP|PROGRESS|{i+1}/{len(bulletins)}|Processing bulletin for {pub_date}")
                    
                    # Process the bulletin
                    incidents_count = await self._process_single_bulletin(
                        session, url, pub_date
                    )

                    if status is None:
                        self.stats.bulletins_new += 1
                    self.stats.incidents_extracted += incidents_count

            logger.info(str(self.stats))
            return self.stats

        except Exception as e:
            logger.exception(f"Pipeline failed with error: {e}")
            self.stats.errors_encountered += 1
            raise

    def request_shutdown(self):
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self._shutdown = True


async def run_pipeline_once(
    database_url: Optional[str] = None,
    force_reprocess: bool = False,
) -> PipelineStats:
    """Run the pipeline once and return statistics."""
    setup_logging()
    settings = get_settings()

    # Validate config
    errors = settings.validate()
    if errors:
        for error in errors:
            logger.error(f"Configuration error: {error}")
        raise ValueError(f"Configuration errors: {errors}")

    # Initialize database
    db_manager = init_database(database_url)

    # Create and run pipeline
    pipeline = MVRCrimeBulletinPipeline(db_manager)

    try:
        stats = await pipeline.run(force_reprocess=force_reprocess)
        return stats
    finally:
        await pipeline.close()


def main():
    """Main entry point for CLI usage."""
    import argparse

    parser = argparse.ArgumentParser(description="MVR Crime Bulletin Pipeline")
    parser.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Re-process already processed bulletins"
    )
    parser.add_argument(
        "--database-url",
        help="SQLAlchemy database URL"
    )
    args = parser.parse_args()

    stats = asyncio.run(run_pipeline_once(
        database_url=args.database_url,
        force_reprocess=args.force_reprocess,
    ))

    # Exit with error code if there were errors
    sys.exit(0 if stats.errors_encountered == 0 else 1)


if __name__ == "__main__":
    main()
