#!/usr/bin/env python3
"""Test fallback parser on real data."""

import sys
sys.path.insert(0, '/Users/opl/Documents/development/finki_masters/Data Visualizations/mvr_scraper')

from extractor import CrimeIncidentExtractor
from database import init_database, ProcessingError

# Get a failing case from DB
db = init_database()
with db.get_session() as session:
    errors = session.query(ProcessingError).filter(
        ProcessingError.error_type == 'JSONParseError'
    ).all()
    
    if errors:
        error = errors[0]
        print(f"Testing fallback parser on error {error.id}")
        print(f"Raw LLM output ({len(error.raw_llm_output) if error.raw_llm_output else 0} chars):")
        print("-" * 50)
        
        if error.raw_llm_output:
            # Use bulletin text for parsing
            raw_text = error.raw_llm_output
        else:
            # Try to get raw_text from bulletin
            raw_text = None
        
        # Get the raw bulletin text
        if error.bulletin_id:
            from database import Bulletin
            bulletin = session.query(Bulletin).filter(Bulletin.id == error.bulletin_id).first()
            if bulletin:
                raw_text = bulletin.raw_text
        
        if raw_text:
            print(f"Using bulletin text ({len(raw_text)} chars)")
            extractor = CrimeIncidentExtractor()
            incidents = extractor._fallback_parse(raw_text)
            print(f"\nExtracted {len(incidents)} incidents:")
            for i, inc in enumerate(incidents[:5]):  # Show first 5
                print(f"\n{i+1}. Crime: {inc['crime_type'][:60] if inc['crime_type'] else 'N/A'}...")
                print(f"   City: {inc['location_city']}")
                print(f"   Date: {inc['crime_date']}")
                print(f"   Ages: {inc['perpetrator_ages']}")
                print(f"   Count: {inc['perpetrator_count']}")
        else:
            print("No text available to parse")
