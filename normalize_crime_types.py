#!/usr/bin/env python3
"""
Normalize crime types in the database to standard categories.
This fixes variations due to OCR issues or LLM output differences.
"""
import sqlite3
import sys
import os
import re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Standard crime categories - simplified names for better matching
CRIME_TYPE_NORM = {
    # Traffic - ALL traffic-related crimes
    'сообраќајна несреќа': [
        # Original
        'безобзирно управување', 'безобѕирно', 'безо$бзирно',
        'безобзирно', 'безобзирно управување', 'безобзирно управување со моторно возило',
        'безобѕирно управување', 'безобѕирно управување со моторно возило',
        'сообраќајна', 'сообрак', 'несрека', 'несреќ', 'несрека сообраќајна',
        'сообраќајна несрека', 'сообраќајна несрека со моторно возило',
        'сообраќајна несрека со моторно возило и пешахци',
        'сообраќајна несрека со патничко возило',
        'управување', 'управување возило',
        # OCR variations
        'безобзирно', 'безо$бзирно', 'безо6зирно',
    ],
    
    # Drugs - ALL drug-related crimes
    'неовластено производство и пуштање во промет на наркотични дроги': [
        'друго', 'друг', 'дрог', 'дрога', 'наркотичн', 'наркотик',
        'марихуа', 'марихуана', 'кокаин', 'хероин', 'амфетамин',
        'психотропн', 'прекурзори', 'наркотичн дроги',
        'неовластено производство и пуштање во промет на наркотичн',
        'неовластено производство и пуштање во промет на наркотични дроги',
        'држење наркотичн', 'производство наркотичн',
    ],
    
    # Violence - ALL violence-related crimes
    'насилство': [
        'напад', 'насил', 'нападнат', 'насилство', 'биење', 'биени',
        'тешка телесна повреда', 'тешки телесни повреди', 'тешка телесна',
        'физички напад', 'физички нападнат', 'физичка пресметка',
        'нападнат', 'нападнат од', 'биен',
        'тепа', 'тепаат', 'удира', 'удар',
        'physical assault', 'assault', 'violence',
    ],
    
    # Domestic violence
    'домашно насилство': [
        'домашно', 'домашно насилство', 'семејно насилство',
        'закани во семејство', 'насилство во семејство',
    ],
    
    # Theft
    'кража': [
        'кражба', 'кражет', 'краже', 'крат', 'кража на',
        'тешка кражба', 'крадење', 'крадат',
        'theft', 'burglary',
    ],
    
    # Robbery
    'разбој': [
        'разбог', 'разбо', 'грабеж', 'граб', 'разбојство',
        'robbery',
    ],
    
    # Fraud
    'измама': [
        'измам', 'изма', 'превара', 'превараци',
        'fraud',
    ],
    
    # Weapons
    'недозволено изработување, држење и тргување со оружје': [
        'оружје', 'оруж', 'пиштол', 'пиштол', 'пушка', 'пушк',
        'граната', 'гранат', 'експлозив', 'експлози',
        'weapon', 'gun', 'arms',
    ],
    
    # Murder
    'убиство': [
        'убиство', 'уби', 'убиен',
        'murder', 'homicide',
    ],
    
    # Arson
    'предизвикување општа опасност': [
        'палење', 'пожар', 'пожари', 'горење',
        'arson', 'fire',
    ],
    
    # Public order
    'нарушување на јавниот ред и мир': [
        'јавен ред', 'ред и мир', 'мир', 'јавниот ред',
        'нарушува ред', 'претерување',
    ],
    
    # Kidnapping
    'грабнување': [
        'грабнува', 'грабнување', 'киднап',
    ],
    
    # Document forgery
    'фалсификување исправа': [
        'фалсификува', 'фалсифик', 'лажен документ',
    ],
    
    # Extortion
    'изнудување': [
        'изнудува', 'изнудување',
    ],
    
    # Threat
    'загрозување на сигурноста': [
        'закана', 'закани', 'загрозува',
    ],
}


def normalize_crime_type(crime_type):
    """Normalize a crime type to standard category."""
    if not crime_type:
        return crime_type
    
    crime_lower = crime_type.lower()
    
    for standard, variations in CRIME_TYPE_NORM.items():
        for var in variations:
            if var.lower() in crime_lower:
                return standard
    
    return crime_type


def run_normalization():
    db_path = os.path.join(os.path.dirname(__file__), "mvr_bulletins.db")
    
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return False
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all distinct crime types
    cursor.execute("SELECT DISTINCT crime_type FROM crime_incidents")
    crime_types = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {len(crime_types)} distinct crime types")
    
    # Create normalization mapping
    norm_map = {}
    unchanged = []
    for ct in crime_types:
        normalized = normalize_crime_type(ct)
        if normalized != ct:
            norm_map[ct] = normalized
        else:
            unchanged.append(ct)
    
    print(f"\nWill normalize {len(norm_map)} crime types:")
    for old, new in sorted(norm_map.items()):
        print(f"  '{old}' -> '{new}'")
    
    if not norm_map:
        print("No crime types need normalization!")
    else:
        # Update database
        updated = 0
        for old_type, new_type in norm_map.items():
            cursor.execute("""
                UPDATE crime_incidents 
                SET crime_type = ? 
                WHERE crime_type = ?
            """, (new_type, old_type))
            updated += cursor.rowcount
        
        conn.commit()
        print(f"\nUpdated {updated} records")
    
    # Show new distinct types
    cursor.execute("SELECT COUNT(DISTINCT crime_type) FROM crime_incidents")
    new_count = cursor.fetchone()[0]
    print(f"Now have {new_count} distinct crime types")
    
    # Show remaining uncategorized types
    print(f"\nUncategorized ({len(unchanged)}):")
    for ct in sorted(unchanged)[:10]:
        print(f"  - {ct}")
    if len(unchanged) > 10:
        print(f"  ... and {len(unchanged) - 10} more")
    
    conn.close()
    return True


if __name__ == "__main__":
    success = run_normalization()
    sys.exit(0 if success else 1)
