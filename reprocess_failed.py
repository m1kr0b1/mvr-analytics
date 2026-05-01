#!/usr/bin/env python3
"""
Reprocess bulletins that failed during initial ingestion.
Uses increased max_tokens and truncated JSON recovery.
"""
import asyncio
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timezone
from extractor import CrimeIncidentExtractor, JSONParseError

async def reprocess_single_bulletin(cursor, bulletin_id, url, pub_date, raw_text):
    """Reprocess a single bulletin."""
    extractor = CrimeIncidentExtractor()
    
    try:
        # Extract incidents
        incidents = await extractor.extract_incidents(raw_text)
        return bulletin_id, True, incidents, None
    except Exception as e:
        return bulletin_id, False, [], str(e)
    finally:
        await extractor.close()


async def reprocess_failed_bulletins():
    """Reprocess all bulletins that had JSON parse errors."""
    db_path = os.path.join(os.path.dirname(__file__), "mvr_bulletins.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Find failed/pending bulletins
    cursor.execute("""
        SELECT b.id, b.url, b.publication_date, b.raw_text
        FROM bulletins b
        INNER JOIN processing_errors pe ON b.id = pe.bulletin_id
        WHERE pe.error_type = 'JSONParseError'
        AND b.status = 'PENDING'
    """)
    failed_bulletins = cursor.fetchall()
    
    print(f"Found {len(failed_bulletins)} bulletins to reprocess\n")
    
    if not failed_bulletins:
        conn.close()
        return
    
    success_count = 0
    fail_count = 0
    
    for bulletin_id, url, pub_date, raw_text in failed_bulletins:
        print(f"Reprocessing bulletin {bulletin_id} ({pub_date})...")
        
        bulletin_id, success, incidents, error = await reprocess_single_bulletin(
            cursor, bulletin_id, url, pub_date, raw_text
        )
        
        if success and incidents:
            print(f"  ✓ Extracted {len(incidents)} incidents")
            
            # Delete old processing errors
            cursor.execute("DELETE FROM processing_errors WHERE bulletin_id = ?", (bulletin_id,))
            
            # Delete old incidents
            cursor.execute("DELETE FROM crime_incidents WHERE bulletin_id = ?", (bulletin_id,))
            
            # Insert new incidents
            for incident_data in incidents:
                address = incident_data.get("location_address")
                cursor.execute("""
                    INSERT INTO crime_incidents (
                        bulletin_id, crime_type, crime_date, location_city, 
                        location_address, perpetrator_count, perpetrator_ages, 
                        perpetrator_gender, outcome, raw_text, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bulletin_id,
                    incident_data["crime_type"],
                    incident_data.get("crime_date"),
                    incident_data["location_city"],
                    address,
                    incident_data["perpetrator_count"],
                    str(incident_data.get("perpetrator_ages", [])),
                    incident_data["perpetrator_gender"],
                    incident_data.get("outcome"),
                    incident_data["raw_text"],
                    datetime.now(timezone.utc).isoformat()
                ))
            
            # Update bulletin status
            cursor.execute("""
                UPDATE bulletins SET status = 'PROCESSED', processed_at = ? WHERE id = ?
            """, (datetime.now(timezone.utc).isoformat(), bulletin_id))
            
            conn.commit()
            success_count += 1
        else:
            print(f"  ✗ Error: {error}")
            fail_count += 1
    
    conn.close()
    
    print(f"\n{'='*50}")
    print(f"Reprocessing complete!")
    print(f"  ✓ Successfully reprocessed: {success_count}")
    print(f"  ✗ Failed: {fail_count}")
    
    if success_count > 0:
        print(f"\nYou can now run 'python migrate_add_precise_coords.py' to geocode the new incidents")


if __name__ == "__main__":
    asyncio.run(reprocess_failed_bulletins())
