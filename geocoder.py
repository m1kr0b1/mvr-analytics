"""
Geocoding service for Macedonia locations.
Handles villages, streets, and specific addresses with caching.
"""
import json
import logging
import time
import os
from pathlib import Path
from typing import Optional, Tuple
from functools import lru_cache

logger = logging.getLogger(__name__)

# Try to import requests, fall back to urllib if needed
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# Cache file location
CACHE_DIR = Path(__file__).parent / ".geocode_cache"
CACHE_FILE = CACHE_DIR / "address_coords.json"

# Nominatim API settings (OpenStreetMap)
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_REVERSE = "https://nominatim.openstreetmap.org/reverse"

# Rate limiting
LAST_REQUEST_TIME = 0
MIN_REQUEST_INTERVAL = 1.1  # Nominatim requires 1 request/second

# Macedonia bounding box for validation
MACEDONIA_BOUNDS = {
    "min_lat": 40.8,
    "max_lat": 42.5,
    "min_lon": 20.0,
    "max_lon": 23.1,
}


def _load_cache() -> dict:
    """Load geocoding cache from file."""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load geocode cache: {e}")
    return {}


def _save_cache(cache: dict):
    """Save geocoding cache to file."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Could not save geocode cache: {e}")


def _rate_limit():
    """Apply rate limiting for API requests."""
    global LAST_REQUEST_TIME
    elapsed = time.time() - LAST_REQUEST_TIME
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    LAST_REQUEST_TIME = time.time()


def _is_in_macedonia(lat: float, lon: float) -> bool:
    """Check if coordinates are within Macedonia."""
    return (
        MACEDONIA_BOUNDS["min_lat"] <= lat <= MACEDONIA_BOUNDS["max_lat"] and
        MACEDONIA_BOUNDS["min_lon"] <= lon <= MACEDONIA_BOUNDS["max_lon"]
    )


def geocode_address(address: str, city: str = None) -> Optional[Tuple[float, float]]:
    """
    Geocode an address to coordinates.
    
    Args:
        address: Street/village address (e.g., "с.Камењане", "бул. Партизански")
        city: Parent city for context (e.g., "Тетово")
    
    Returns:
        Tuple of (lat, lon) or None if not found
    """
    if not address or not address.strip():
        return None
    
    address = address.strip()
    cache = _load_cache()
    
    # Check cache first
    cache_key = f"{address}|{city or ''}"
    if cache_key in cache:
        result = cache[cache_key]
        if result:
            return (result["lat"], result["lon"])
        return None
    
    # Clean up address for lookup
    clean_address = _clean_address(address)
    if not clean_address:
        cache[cache_key] = None
        _save_cache(cache)
        return None
    
    # Try forward geocoding with Nominatim
    coords = _nominatim_geocode(clean_address, city)
    
    # Cache the result
    if coords:
        cache[cache_key] = {"lat": coords[0], "lon": coords[1], "address": clean_address}
    else:
        cache[cache_key] = None
    _save_cache(cache)
    
    return coords


def _clean_address(address: str) -> Optional[str]:
    """Clean and normalize an address for geocoding."""
    if not address:
        return None
    
    # Remove common prefixes/patterns
    address = address.strip()
    
    # Skip very short addresses
    if len(address) < 3:
        return None
    
    # Skip generic addresses
    skip_patterns = [
        "угостителски", "објек", "атар", "територи", "пат",
        "национален пат", "регионален пат", "локал",
        "непознато", "unknow", "unknown"
    ]
    address_lower = address.lower()
    for pattern in skip_patterns:
        if pattern in address_lower:
            # Still might be useful, but skip very generic ones
            if len(address) < 10:
                return None
    
    return address


def _nominatim_geocode(address: str, city: str = None) -> Optional[Tuple[float, float]]:
    """Use Nominatim to geocode an address."""
    if not HAS_REQUESTS:
        logger.warning("requests library not available for geocoding")
        return None
    
    # Build query with Macedonia country
    query_parts = [address]
    if city:
        query_parts.append(city)
    query_parts.append("Macedonia")
    query = ", ".join(query_parts)
    
    try:
        _rate_limit()
        
        response = requests.get(
            NOMINATIM_URL,
            params={
                "q": query,
                "format": "json",
                "limit": 1,
                "countrycodes": "mk",
                "addressdetails": 0,
            },
            headers={
                "User-Agent": "MVR-Crime-Analytics/1.0 (research project)",
                "Accept": "application/json",
            },
            timeout=10,
        )
        
        if response.status_code == 200:
            results = response.json()
            if results and len(results) > 0:
                lat = float(results[0]["lat"])
                lon = float(results[0]["lon"])
                
                # Validate coordinates are in Macedonia
                if _is_in_macedonia(lat, lon):
                    logger.debug(f"Geocoded '{address}' to ({lat}, {lon})")
                    return (lat, lon)
                else:
                    logger.debug(f"Geocoded '{address}' but outside Macedonia: ({lat}, {lon})")
        
        # Try simpler query if full query failed
        if city and city != address:
            return _nominatim_geocode(address, None)
        
    except Exception as e:
        logger.debug(f"Nominatim geocode error for '{address}': {e}")
    
    return None


def reverse_geocode(lat: float, lon: float) -> Optional[str]:
    """
    Reverse geocode coordinates to an address.
    
    Args:
        lat: Latitude
        lon: Longitude
    
    Returns:
        Address string or None if not found
    """
    if not lat or not lon:
        return None
    
    cache = _load_cache()
    cache_key = f"reverse|{lat:.5f}|{lon:.5f}"
    
    if cache_key in cache:
        return cache[cache_key]
    
    if not HAS_REQUESTS:
        return None
    
    try:
        _rate_limit()
        
        response = requests.get(
            NOMINATIM_REVERSE,
            params={
                "lat": lat,
                "lon": lon,
                "format": "json",
                "addressdetails": 1,
            },
            headers={
                "User-Agent": "MVR-Crime-Analytics/1.0 (research project)",
            },
            timeout=10,
        )
        
        if response.status_code == 200:
            result = response.json()
            if result and "display_name" in result:
                address = result["display_name"]
                cache[cache_key] = address
                _save_cache(cache)
                return address
        
    except Exception as e:
        logger.debug(f"Reverse geocode error: {e}")
    
    return None


# Known village coordinates for quick lookup
# This covers common villages mentioned in bulletins
VILLAGE_COORDS = {
    # Tetovo area
    "с. Камењане": (42.0667, 20.9667),
    "с.Камењане": (42.0667, 20.9667),
    "Камењане": (42.0667, 20.9667),
    "с. Симница": (41.8500, 20.9000),
    "с.Беловиште": (42.0167, 20.9500),
    "с. Рогатско": (42.0333, 20.9167),
    "с. Желино": (42.0500, 21.0500),
    "с. Теарце": (42.0667, 21.0333),
    "с. Слупчане": (42.0833, 21.0667),
    
    # Gostivar area
    "с. Симница": (41.8500, 20.9000),
    "с. Турек": (41.8000, 20.8667),
    "с. Врапчиште": (41.8667, 20.8667),
    "с. Чегране": (41.7833, 20.9000),
    "с. Бабушница": (41.8167, 20.8333),
    
    # Skopje area villages
    "с. Морани": (41.9167, 21.4833),
    "с.Морани": (41.9167, 21.4833),
    "с. Чифлик": (41.9500, 21.4500),
    "с. Јурумлери": (41.9333, 21.4833),
    "с. Кучевиште": (42.0167, 21.4000),
    "с. Радишани": (41.9333, 21.4167),
    "с. Сопиште": (41.8833, 21.4833),
    "с. Сандево": (42.0167, 21.3833),
    "с. Петровец": (41.9500, 21.6000),
    "с.Батинци": (41.8833, 21.5167),
    "с.Батинци": (41.9167, 21.5000),
    "с. Трубарево": (41.9167, 21.4500),
    "с.Марино": (41.8833, 21.5333),
    "с.Подгорци": (42.0500, 21.4000),
    
    # Bitola area
    "с. Крушево": (41.2500, 21.3500),
    "с. Островица": (41.2000, 21.3000),
    "с. Бахарно": (41.1500, 21.3500),
    "с. Требош": (41.0667, 21.0167),
    "с. Брусник": (41.1333, 21.3500),
    
    # Strumica area
    "с. Колешино": (41.4500, 22.7500),
    "с.Колешино": (41.4500, 22.7500),
    "с. Неманици": (41.4167, 22.7000),
    "с. Доброшинци": (41.4500, 22.6333),
    "с. Стримница": (41.4333, 22.7000),
    "с.Ижиште": (41.5000, 22.6500),
    "с. Спанчево": (41.4500, 22.7000),
    "с. Русјаци": (41.4000, 22.7500),
    "с.Ботун": (41.4167, 22.7167),
    "с.Подмољје": (41.3667, 22.7000),
    "с.Преглово": (41.3833, 22.6500),
    "с.Радање": (41.3667, 22.7333),
    "с.Слатино": (41.3500, 22.6500),
    "с.Жиганци": (41.4167, 22.8000),
    
    # Kumanovo area
    "с. Трновац": (42.0833, 21.7167),
    "с.Трновац": (42.0833, 21.7167),
    "с. Куманово": (42.1322, 21.7145),
    "с. Старо Нагоричане": (42.1167, 21.8167),
    "с. Прждево": (42.0500, 21.7833),
    "с. Соколарци": (42.0667, 21.8333),
    "с. Волково": (42.1000, 21.7500),
    
    # Other regions
    "с. Богдање": (41.5000, 21.2000),
    "с. Логодари": (41.4500, 21.3000),
    "с. Долно Палчиште": (41.6167, 21.3667),
    "с. Оровник": (41.2500, 20.9500),
    "с. Волино": (41.4000, 21.0000),
    "с. Ливада": (41.3833, 21.0333),
    "с. Мешеишта": (41.3667, 21.0167),
    "с. Таринци": (41.2833, 21.2000),
    "с. Фурка": (41.3500, 21.2833),
}


def get_village_coords(village_name: str) -> Optional[Tuple[float, float]]:
    """Quick lookup for known village coordinates."""
    if not village_name:
        return None
    
    # Normalize input
    normalized = village_name.strip()
    
    # Direct match in known villages
    if normalized in VILLAGE_COORDS:
        return VILLAGE_COORDS[normalized]
    
    # Try without "с." prefix
    without_prefix = normalized
    for prefix in ["с. ", "с.", "село ", "с. ", "village "]:
        if normalized.startswith(prefix):
            without_prefix = normalized[len(prefix):].strip()
            break
    
    if without_prefix in VILLAGE_COORDS:
        return VILLAGE_COORDS[without_prefix]
    
    # Try with prefix
    with_prefix = f"с. {without_prefix}"
    if with_prefix in VILLAGE_COORDS:
        return VILLAGE_COORDS[with_prefix]
    
    return None


def geocode_location(city: str, address: str = None) -> Optional[Tuple[float, float]]:
    """
    Main geocoding function - tries multiple strategies.
    
    Args:
        city: City/municipality name
        address: Specific location (village, street, etc.)
    
    Returns:
        Tuple of (lat, lon) or None
    """
    # 1. If we have a specific address, try that first
    if address:
        # Check known villages
        coords = get_village_coords(address)
        if coords:
            return coords
        
        # Try geocoding the address
        coords = geocode_address(address, city)
        if coords:
            return coords
    
    # 2. Fall back to city coordinates
    from macedonia_coords import get_coords
    coords = get_coords(city)
    if coords:
        return coords
    
    return None


def get_cache_stats() -> dict:
    """Get statistics about the geocoding cache."""
    cache = _load_cache()
    total = len(cache)
    successful = sum(1 for v in cache.values() if v is not None)
    return {
        "total_lookups": total,
        "successful": successful,
        "cache_hit_rate": f"{(successful/total*100):.1f}%" if total > 0 else "N/A"
    }


if __name__ == "__main__":
    # Test geocoding
    test_addresses = [
        ("Тетово", "с. Камењане"),
        ("Кратово", "село Трновац"),
        ("Скопје", "Бит Пазар"),
        ("Струмица", "атар на село Колешино"),
    ]
    
    print("Testing geocoding:")
    for city, addr in test_addresses:
        coords = geocode_location(city, addr)
        print(f"  {addr} ({city}): {coords}")
