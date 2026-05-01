#!/usr/bin/env python3
"""
Quick geocode for incidents that don't have precise coordinates yet.
"""
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from geocoder import geocode_location

def geocode_new_incidents():
    db_path = os.path.join(os.path.dirname(__file__), "mvr_bulletins.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Find incidents needing geocoding
    cursor.execute("""
        SELECT id, location_city, location_address 
        FROM crime_incidents 
        WHERE location_address IS NOT NULL 
        AND location_address != ''
        AND (precise_lat IS NULL OR precise_lon IS NULL)
    """)
    
    to_geocode = cursor.fetchall()
    print(f"Found {len(to_geocode)} incidents needing geocoding...")
    
    if not to_geocode:
        print("No new incidents to geocode.")
        conn.close()
        return
    
    geocoded = 0
    for inc_id, city, address in to_geocode:
        if address and len(str(address).strip()) >= 3:
            coords = geocode_location(city, address)
            if coords:
                cursor.execute("""
                    UPDATE crime_incidents 
                    SET precise_lat = ?, precise_lon = ? 
                    WHERE id = ?
                """, (coords[0], coords[1], inc_id))
                geocoded += 1
                
                if geocoded % 20 == 0:
                    conn.commit()
                    print(f"  Geocoded {geocoded}/{len(to_geocode)}...")
    
    conn.commit()
    print(f"\nGeocoding complete! {geocoded} incidents geocoded.")
    
    # Stats
    cursor.execute("""
        SELECT COUNT(*) FROM crime_incidents 
        WHERE precise_lat IS NOT NULL AND precise_lon IS NOT NULL
    """)
    with_coords = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM crime_incidents")
    total = cursor.fetchone()[0]
    
    print(f"Total: {with_coords}/{total} records have precise coordinates ({with_coords/total*100:.1f}%)")
    
    conn.close()

if __name__ == "__main__":
    geocode_new_incidents()
