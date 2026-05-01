#!/usr/bin/env python3
"""
Migration script to add precise_lat/precise_lon columns to crime_incidents table.
Also populates coordinates for existing records with location_address.
"""
import sqlite3
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from geocoder import geocode_location

def run_migration():
    db_path = os.path.join(os.path.dirname(__file__), "mvr_bulletins.db")
    
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found at {db_path}")
        return False
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if columns already exist
    cursor.execute("PRAGMA table_info(crime_incidents)")
    columns = [col[1] for col in cursor.fetchall()]
    
    # Add columns if they don't exist
    if 'precise_lat' not in columns:
        print("Adding precise_lat column...")
        cursor.execute("ALTER TABLE crime_incidents ADD COLUMN precise_lat REAL")
    
    if 'precise_lon' not in columns:
        print("Adding precise_lon column...")
        cursor.execute("ALTER TABLE crime_incidents ADD COLUMN precise_lon REAL")
    
    conn.commit()
    print("Columns added successfully!")
    
    # Count records with addresses that need geocoding
    cursor.execute("""
        SELECT COUNT(*) FROM crime_incidents 
        WHERE location_address IS NOT NULL 
        AND location_address != '' 
        AND (precise_lat IS NULL OR precise_lon IS NULL)
    """)
    to_geocode = cursor.fetchone()[0]
    print(f"\nFound {to_geocode} records needing geocoding...")
    
    if to_geocode == 0:
        print("No records to geocode.")
        return True
    
    # Geocode existing records
    cursor.execute("""
        SELECT id, location_city, location_address FROM crime_incidents 
        WHERE location_address IS NOT NULL 
        AND location_address != '' 
        AND (precise_lat IS NULL OR precise_lon IS NULL)
    """)
    
    records = cursor.fetchall()
    geocoded = 0
    failed = 0
    
    for record_id, city, address in records:
        if not address or len(address.strip()) < 3:
            failed += 1
            continue
        
        coords = geocode_location(city, address)
        if coords:
            cursor.execute("""
                UPDATE crime_incidents 
                SET precise_lat = ?, precise_lon = ? 
                WHERE id = ?
            """, (coords[0], coords[1], record_id))
            geocoded += 1
            
            if geocoded % 10 == 0:
                conn.commit()
                print(f"  Geocoded {geocoded}/{len(records)}...")
        else:
            failed += 1
    
    conn.commit()
    print(f"\nMigration complete!")
    print(f"  Successfully geocoded: {geocoded}")
    print(f"  Failed/skipped: {failed}")
    
    # Print stats
    cursor.execute("""
        SELECT COUNT(*) FROM crime_incidents 
        WHERE precise_lat IS NOT NULL AND precise_lon IS NOT NULL
    """)
    with_coords = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM crime_incidents")
    total = cursor.fetchone()[0]
    
    print(f"\nDatabase now has precise coordinates for {with_coords}/{total} records ({with_coords/total*100:.1f}%)")
    
    conn.close()
    return True


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
