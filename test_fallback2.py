#!/usr/bin/env python3
"""Debug the fallback parser."""

import sys
sys.path.insert(0, '/Users/opl/Documents/development/finki_masters/Data Visualizations/mvr_scraper')

from database import init_database, ProcessingError, Bulletin
import re

# Get a failing case from DB
db = init_database()
with db.get_session() as session:
    errors = session.query(ProcessingError).filter(
        ProcessingError.error_type == 'JSONParseError'
    ).all()
    
    if errors:
        error = errors[0]
        
        if error.bulletin_id:
            bulletin = session.query(Bulletin).filter(Bulletin.id == error.bulletin_id).first()
            if bulletin:
                raw_text = bulletin.raw_text
                print(f"Bulletin text ({len(raw_text)} chars):")
                print("=" * 50)
                print(raw_text[:3000])
                print("\n\nLooking for patterns...")
                
                # Check for city mentions
                cities = ['Скопје', 'Свети Николе', 'Виница', 'Охрид']
                for city in cities:
                    if city in raw_text:
                        print(f"Found city: {city}")
                
                # Check for date patterns
                dates = re.findall(r'\d{1,2}\.\d{1,2}\.\d{4}', raw_text)
                print(f"Found dates: {dates[:10]}")
                
                # Check for crime pattern
                crime_match = re.search(r'кривично дело\s+"([^"]+)"', raw_text)
                if crime_match:
                    print(f"Found crime: {crime_match.group(1)}")
