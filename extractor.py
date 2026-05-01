"""
LLM Extraction layer for MVR Crime Bulletin scraper.
Sends raw bulletin text to LLM and parses structured crime incident data.
"""
import json
import logging
import re
from datetime import date, datetime
from typing import Optional, List, Dict, Any

from openai import AsyncOpenAI, OpenAIError

from config import get_settings

logger = logging.getLogger(__name__)


# Global crime categories - mapping variations to standard categories
CRIME_CATEGORY_MAPPING = {
    # Violence / Assault - map all physical violence to one category
    "насилство": [
        "насилство",  # Direct match
        "физички напад", "физичка пресметка", "напад", "напад на", "насилство врз дете",
        "тешка телесна повреда", "тешки телесни повреди", "бадијална повреда",
        "физичко нападение", "напад на лице", "физички нападнат", "нападнат",
        "употреба на", "тепа", "удира", "physical assault", "assault", "violence"
    ],
    "домашно насилство": [
        "домашно насилство", "семејно насилство", "насилство во семејство",
        "закани во семејство", "насилство врз", "домашно"
    ],
    "убиство": ["убиство", "убиство на", "лишување од живот"],
    
    # Property crimes
    "кража": ["кража", "тешка кражба", "крадење", "крадат"],
    "разбој": ["разбој", "разбојни", "грабеж"],
    "измама": ["измама", "измами", "превара"],
    
    # Drug crimes - use PRODUCTION/TRAFFICKING, NOT the substance
    "неовластено производство и пуштање во промет на наркотични дроги": [
        "наркотичн", "марихуана", "кокаин", "хероин", "амфетамин", 
        "психотропн", "прекурзори", "држење наркотич", "производство наркотич",
        "пуштање во промет", "промет на наркотич", "транспорт на дрога",
        "бел прашкаста", "дрога", "дроги"
    ],
    
    # Weapons
    "недозволено изработување, држење и тргување со оружје": [
        "оружи", "пиштол", "пушка", "граната", "експлозив", 
        "артилериск", "закана со оружје"
    ],
    
    # Traffic
    "сообраќајна несреќа": ["сообраќајна несреќа", "несреќа со", "тешки телесни повреди"],
    "управување под дејство на алкохол": ["алкохол", "алкохолизиран", "пијан"],
    
    # Public order
    "нарушување на јавниот ред и мир": ["јавен ред", "нарушува ред", "мир", "претерување"],
    
    # Document crimes
    "фалсификување исправа": ["фалсификува", "лажен документ"],
    
    # Property/building
    "бесправно градење": ["бесправно градење", "незаконска градеж"],
    "узурпација": ["узурпац", "недозволено заземање", "неовластено заземање"],
    
    # Other common crimes
    "предизвикување општа опасност": ["општа опасност", "пожар", "палење", "електрична"],
    "злоупотреба на службена положба": ["злоупотреба службен", "корупци", "службена положба"],
    "попречување на правдата": ["попречување правдата", "сведочење"],
    "загрозување на сигурноста": ["загрозување сигурно", "закана", "тероризам"],
    "злоупотреба на лични податоци": ["лични податоци", "заштита на податоци"],
    "компјутерски криминал": ["компјутерск", "хакира", "cyber", "вирус"],
}

# Phrases that are NOT crime types - skip these
NON_CRIME_PHRASES = [
    "апси", "уапсен", "притвор", "притворе", "затвор",
    "бел", "прашкаста материја",  # Items found, not crimes
    "боллс", "голф", "голф 6", "мерцедес", "опел", "демиркаписко",  # Car models
    "дневни билтени", "полициски справочник",
    "барање по судска наредба", "по барање", "по бара",
    "претрес", "претресува", "спроведува",  # Actions, not crimes
    "кривична пријава", "кривично дело",  # Actions
    "сомнение за",  # Unconfirmed
    "полици", "полицейск",  # Processing artifacts
    "предмет", "предметна", "предметное",  # Incomplete/corrupted
    "undefined", "null", "none", "nan",
]


def normalize_crime_type(crime_type: str) -> str:
    """
    Normalize a crime type string to a standard category.
    Maps variations to global categories, filters out non-crimes.
    Also handles English outputs from LLM.
    """
    if not crime_type or not isinstance(crime_type, str):
        return "неозначено"
    
    crime_lower = crime_type.lower().strip()
    crime_title = crime_type.strip().title()  # For Macedonian title case
    
    # ENGLISH -> Macedonian mapping for when LLM returns English
    ENGLISH_TO_MACEDONIAN = {
        "physical assault": "насилство",
        "assault": "насилство",
        "stabb": "насилство",  # stabbing
        "violence": "насилство",
        "theft": "кража",
        "burglary": "кража",
        "robbery": "разбој",
        "fraud": "измама",
        "drug": "неовластено производство и пуштање во промет на наркотични дроги",
        "narcotic": "неовластено производство и пуштање во промет на наркотични дроги",
        "weapon": "недозволено изработување, држење и тргување со оружје",
        "gun": "недозволено изработување, држење и тргување со оружје",
        "traffic accident": "сообраќајна несреќа",
        "dui": "управување под дејство на алкохол",
        "drunk driving": "управување под дејство на алкохол",
        "murder": "убиство",
        "homicide": "убиство",
        "vandalism": "предизвикување општа опасност",
        "arson": "предизвикување општа опасност",
        "domestic violence": "домашно насилство",
        "fraud": "измама",
        "identity theft": "злоупотреба на лични податоци",
        "cyber crime": "компјутерски криминал",
    }
    
    # Check for English crime types
    for eng, maced in ENGLISH_TO_MACEDONIAN.items():
        if eng in crime_lower:
            return maced
    
    # Skip very short entries
    if len(crime_type) < 5:
        return None
    
    # Check for non-crime phrases
    for phrase in NON_CRIME_PHRASES:
        if phrase in crime_lower:
            return None
    
    # Check against category mapping (case-insensitive)
    for global_category, keywords in CRIME_CATEGORY_MAPPING.items():
        for keyword in keywords:
            if keyword in crime_lower:
                return global_category
    
    # If no match, check if it looks like a valid crime (has reasonable length)
    if len(crime_type) >= 10:
        return crime_type.strip()  # Keep original if it seems valid
    
    return None


EXTRACTION_SYSTEM_PROMPT = """You are an expert at reading Macedonian police bulletins and extracting structured crime incident data.

CRITICAL: CLASSIFY CRIMES CORRECTLY

1. CRIME TYPES MUST BE FROM MACEDONIAN CRIMINAL LAW:
   - "Уапсен" / "Апси" is NOT a crime - find the underlying offense (theft, assault, etc.)
   - "Марихуана пронајдена" is NOT a crime - the crime is drug trafficking
   - "Бел прашкаст материјал" is NOT a crime - it's evidence of a crime
   - "Претрес" / "Search" is NOT a crime - it's a police action
   - "Кривична пријава" is NOT a crime - it's the action taken
   - Car models (Опел, Голф, Мерцедес) are NOT crimes
   - Items found (дробилка, боллс) are NOT crimes

2. MAP ALL VARIATIONS TO STANDARD CATEGORIES:

   VIOLENCE → "насилство"
   - физички напад, напад, физичка пресметка
   - тешка телесна повреда → "насилство"
   
   DOMESTIC VIOLENCE → "домашно насилство"
   - домашно насилство (закани, физичко, итн) → always "домашно насилство"
   
   DRUGS → "неовластено производство и пуштање во промет на наркотични дроги"
   - ANY mention of marijuana, cocaine, heroin → same category
   - NEVER use the drug name as the crime type
   
   WEAPONS → "недозволено изработување, држење и тргување со оружје"
   - пиштол, пушка, граната, артилериски
   
   TRAFFIC → "сообраќајна несреќа"
   - сообраќајна несреќа (with or without injuries)
   
   PROPERTY → "кража", "разбој", "измама"

3. ONLY CREATE NEW CATEGORIES FOR REAL CRIMES:
   - If unsure, try to fit into existing categories first
   - Return the standardized category, not the raw text

For each incident extract:
- crime_type: STANDARDIZED category (see above)
- crime_date: When crime occurred (DD.MM.YYYY)
- location_city: City/municipality
- location_address: Street or null
- perpetrator_count: "single" or "multiple"
- perpetrator_ages: [ages] or []
- perpetrator_gender: "male", "female", "mixed", "unknown"
- outcome: Brief result
- raw_text: Original Macedonian paragraph

Return ONLY valid JSON array, no preamble:
[
  {"crime_type": "насилство", "crime_date": "05.04.2026", "location_city": "Скопје", ...},
  {"crime_type": "кража", ...}
]
"""


class ExtractionError(Exception):
    """Base exception for extraction errors."""
    pass


class JSONParseError(ExtractionError):
    """Raised when LLM output cannot be parsed as JSON."""
    def __init__(self, message, raw_output=None):
        super().__init__(message)
        self.raw_output = raw_output


class LLMAPIError(ExtractionError):
    """Raised when the LLM API call fails."""
    pass


class CrimeIncidentExtractor:
    """
    Extracts structured crime incidents from raw bulletin text using LLM.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
    ):
        settings = get_settings()
        self.api_key = api_key or settings.openrouter_api_key
        self.base_url = base_url or settings.openrouter_base_url
        self.model = model or settings.llm_model

        if not self.api_key:
            raise ValueError("API key is required for LLM extraction")

        self._client: Optional[AsyncOpenAI] = None

    @property
    def client(self) -> AsyncOpenAI:
        """Get or create the OpenAI client."""
        if self._client is None:
            self._client = AsyncOpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
            )
        return self._client

    async def close(self):
        """Close the API client."""
        if self._client:
            await self._client.close()
            self._client = None

    def _clean_json_response(self, text: str) -> str:
        """Clean LLM response to extract valid JSON."""
        if not text or not text.strip():
            raise JSONParseError("Empty response")
        
        original_text = text
        
        # Remove markdown code fences
        text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'\s*```$', '', text, flags=re.MULTILINE)
        
        # Remove common prefixes
        text = re.sub(r'^(?:here\s*is\s*(?:the\s*)?(?:json|output|data)[:\s]*|json\s*output[:\s]*)', '', text, flags=re.IGNORECASE)
        
        # Find JSON start
        first_array = text.find('[')
        first_object = text.find('{')

        if first_array == -1 and first_object == -1:
            raise JSONParseError(f"No JSON found. Response preview: {text[:200]}")

        start = min(x for x in [first_array, first_object] if x >= 0)
        text = text[start:]
        
        # Find matching end bracket
        if text.startswith('['):
            depth = 0
            for i, char in enumerate(text):
                if char == '[':
                    depth += 1
                elif char == ']':
                    depth -= 1
                    if depth == 0:
                        result = text[:i+1]
                        try:
                            json.loads(result)
                            return result
                        except:
                            pass
        elif text.startswith('{'):
            depth = 0
            for i, char in enumerate(text):
                if char == '{':
                    depth += 1
                elif char == '}':
                    depth -= 1
                    if depth == 0:
                        result = text[:i+1]
                        try:
                            json.loads(result)
                            return result
                        except:
                            pass
        
        raise JSONParseError(f"Could not parse JSON. Preview: {text[:300]}")

    def _extract_valid_objects_from_truncated(self, text: str) -> list:
        """
        Extract valid JSON objects from potentially truncated response.
        Useful when LLM response is cut off mid-JSON.
        """
        valid_objects = []
        
        # Try to find all complete JSON objects (even if array is incomplete)
        i = 0
        while i < len(text):
            # Find next object start
            obj_start = text.find('{', i)
            if obj_start == -1:
                break
            
            # Find matching close brace
            depth = 0
            obj_end = -1
            for j in range(obj_start, len(text)):
                if text[j] == '{':
                    depth += 1
                elif text[j] == '}':
                    depth -= 1
                    if depth == 0:
                        obj_end = j + 1
                        break
            
            if obj_end > obj_start:
                # Try to parse this object
                try:
                    obj = json.loads(text[obj_start:obj_end])
                    valid_objects.append(obj)
                    i = obj_end
                except:
                    i = obj_start + 1
            else:
                # Object not complete (truncated)
                i += 1
        
        return valid_objects

    def _parse_incident_dates(self, incident: dict) -> dict:
        """Parse crime_date from DD.MM.YYYY format to date object."""
        crime_date_str = incident.get("crime_date")
        if crime_date_str and isinstance(crime_date_str, str):
            try:
                parts = crime_date_str.split(".")
                if len(parts) == 3:
                    day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                    incident["crime_date"] = date(year, month, day)
            except (ValueError, IndexError):
                incident["crime_date"] = None
        return incident

    def _validate_incident(self, incident: dict) -> dict:
        """Validate and normalize an incident record."""
        required_fields = [
            "crime_type", "location_city", "perpetrator_count",
            "perpetrator_ages", "perpetrator_gender", "raw_text"
        ]

        for field in required_fields:
            if field not in incident:
                incident[field] = None

        # Normalize crime type
        crime_type = incident.get("crime_type")
        if crime_type:
            normalized = normalize_crime_type(crime_type)
            if normalized:
                incident["crime_type"] = normalized
            else:
                incident["crime_type"] = "друго"

        # Normalize perpetrator_count
        if incident["perpetrator_count"] not in ["single", "multiple"]:
            incident["perpetrator_count"] = "unknown"

        # Normalize perpetrator_gender
        valid_genders = ["male", "female", "mixed", "unknown"]
        if incident["perpetrator_gender"] not in valid_genders:
            incident["perpetrator_gender"] = "unknown"

        # Ensure perpetrator_ages is a list
        if not isinstance(incident.get("perpetrator_ages"), list):
            incident["perpetrator_ages"] = []

        # Parse dates
        incident = self._parse_incident_dates(incident)

        return incident

    async def extract_incidents(self, bulletin_text: str) -> List[dict]:
        """
        Extract crime incidents from bulletin text using LLM.

        Args:
            bulletin_text: The raw Macedonian bulletin text

        Returns:
            List of incident dictionaries

        Raises:
            JSONParseError: If response cannot be parsed as JSON
            LLMAPIError: If the API call fails
        """
        logger.info(f"Extracting incidents from {len(bulletin_text)} characters of text")

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract all crime incidents from this bulletin:\n\n{bulletin_text}"}
                ],
                temperature=0.1,
                max_tokens=16384,  # Increased to handle long bulletins with many incidents
            )

            raw_output = response.choices[0].message.content
            logger.debug(f"LLM raw output length: {len(raw_output)} characters")
            
            self._last_raw_output = raw_output

            # Clean and parse JSON
            try:
                cleaned_json = self._clean_json_response(raw_output)
                incidents = json.loads(cleaned_json)
            except JSONParseError as e:
                e.raw_output = raw_output
                # Try to extract individual valid objects from truncated response
                print(f"\n[JSON PARSE ERROR] Trying truncated response recovery...")
                valid_objects = self._extract_valid_objects_from_truncated(raw_output)
                if valid_objects:
                    print(f"[RECOVERY] Successfully extracted {len(valid_objects)} complete objects from truncated response")
                    incidents = valid_objects
                else:
                    print(f"[JSON PARSE ERROR] Raw LLM output ({len(raw_output)} chars):\n{raw_output[:3000]}\n...")
                    raise

            # Validate incidents
            validated_incidents = []
            for incident in incidents:
                validated_incidents.append(self._validate_incident(incident))

            logger.info(f"Extracted {len(validated_incidents)} crime incidents")
            return validated_incidents

        except OpenAIError as e:
            logger.error(f"LLM API error: {e}")
            raise LLMAPIError(f"OpenAI API error: {e}") from e

        except JSONParseError as e:
            if not hasattr(e, 'raw_output') or not e.raw_output:
                e.raw_output = None
            
            # Try fallback regex parser
            print(f"[FALLBACK] Trying regex parser on bulletin text...")
            fallback_incidents = self._fallback_parse(bulletin_text)
            if fallback_incidents:
                print(f"[FALLBACK] Success! Extracted {len(fallback_incidents)} incidents via regex")
                validated = [self._validate_incident(inc) for inc in fallback_incidents]
                return validated
            
            raise


    def _fallback_parse(self, text: str) -> List[dict]:
        """Fallback regex-based parser for plain text responses."""
        incidents = []
        
        if not text or len(text) < 100:
            return []
        
        # Skip HTML
        text_sample = text[:500].lower()
        if '<!doctype' in text_sample or '<html' in text_sample:
            return []
        
        # Common Macedonian cities
        cities = [
            'Скопје', 'Куманово', 'Битола', 'Прилеп', 'Тетово', 'Велес', 'Охрид',
            'Гостивар', 'Штип', 'Струмица', 'Кавадарци', 'Кочани', 'Кичево',
            'Гевгелија', 'Свети Николе', 'Виница', 'Радовиш', 'Делчево', 'Ресен',
            'Пробиштип', 'Кратово', 'Крушево', 'Македонски Брод', 'Демир Хисар',
            'Берово', 'Валандово', 'Богданци', 'Могила', 'Новаци', 'Липково',
            'Аеродром', 'Чаир', 'Шуто Оризари', 'Карпош', 'Гази Баба',
        ]
        
        # Crime type patterns
        crime_patterns = [
            r'кривично дело\s+"([^"]+)"',
            r'кривично дело\s+[\"\']([^\"\']+)[\"\']',
            r'(?:сторено|извршено)\s+кривично\s+дело\s+["\']?([^\"\'\.,\n]+)',
        ]
        
        # Date patterns
        date_pattern = r'(\d{1,2})\.(\d{1,2})\.(\d{4})'
        
        # Split text into blocks
        blocks = re.split(r'(?=\u041d\u0430\u0434\u0432\u043e\u0440\u0435\u0448\u043d\u0430)', text)
        if len(blocks) < 2:
            blocks = re.split(r'(?<=[.!?])\n\n+', text)
        
        if len(blocks) < 2:
            blocks = [text]
        
        for block in blocks:
            if len(block) < 80:
                continue
            
            incident = {
                'crime_type': None,
                'crime_date': None,
                'location_city': None,
                'location_address': None,
                'perpetrator_count': 'unknown',
                'perpetrator_ages': [],
                'perpetrator_gender': 'unknown',
                'raw_text': block[:500]
            }
            
            # Extract crime type
            for pattern in crime_patterns:
                match = re.search(pattern, block, re.IGNORECASE)
                if match:
                    crime = match.group(1).strip()
                    if len(crime) > 5 and len(crime) < 200:
                        incident['crime_type'] = crime
                        break
            
            # Extract date
            match = re.search(date_pattern, block)
            if match:
                try:
                    day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    if 1 <= day <= 31 and 1 <= month <= 12 and 2020 <= year <= 2030:
                        incident['crime_date'] = date(year, month, day)
                except:
                    pass
            
            # Extract city
            for city in cities:
                if city in block:
                    incident['location_city'] = city
                    break
            
            # Extract ages
            age_matches = re.findall(r'\b(\d{2})\b', block)
            ages = []
            for age in age_matches:
                try:
                    a = int(age)
                    if 10 <= a <= 100:
                        ages.append(a)
                except:
                    pass
            if ages:
                incident['perpetrator_ages'] = list(set(ages))[:5]
                incident['perpetrator_count'] = 'multiple' if len(ages) > 1 else 'single'
            
            # Only keep if has crime type and city
            if incident['crime_type'] or incident['location_city']:
                if incident['crime_type']:
                    incidents.append(incident)
        
        return incidents


async def extract_incidents_async(
    bulletin_text: str,
    api_key: Optional[str] = None,
) -> tuple[List[dict], Optional[str]]:
    """Async function to extract crime incidents."""
    extractor = CrimeIncidentExtractor(api_key=api_key)
    
    try:
        return await extractor.extract_incidents(bulletin_text), None
    except JSONParseError as e:
        return [], str(e)
    except LLMAPIError as e:
        return [], str(e)
    finally:
        await extractor.close()


def extract_incidents_sync(
    bulletin_text: str,
    api_key: Optional[str] = None,
) -> List[dict]:
    """Synchronous wrapper for extract_incidents."""
    return asyncio.get_event_loop().run_until_complete(
        extract_incidents_async(bulletin_text, api_key)
    )


# Also export the normalize function for use elsewhere
__all__ = ['CrimeIncidentExtractor', 'normalize_crime_type', 'EXTRACTION_SYSTEM_PROMPT']