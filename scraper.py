"""
Scraping layer for MVR Crime Bulletin scraper.
Uses Jina AI Reader API for clean content extraction.
"""
import asyncio
import logging
import re
import time
from datetime import date
from typing import Optional, List, Tuple
from urllib.parse import quote, urljoin

import httpx
from bs4 import BeautifulSoup

from config import get_settings

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    """Base exception for scraper errors."""
    pass


class JinaAPIError(ScraperError):
    """Raised when Jina API returns an error."""
    pass


class MVRScraper:
    """
    Scraper for MVR public crime bulletins using Jina AI Reader API.
    
    Jina AI provides clean content extraction from URLs.
    API: https://r.jina.ai/{encoded_url}
    """

    # URL pattern for bulletin pages
    BULLETIN_URL_REGEX = re.compile(
        r'/mk-MK/odnosi-so-javnost/dnevni-bilteni/izvadok-na-del-od-dnevnite-nastani-(\d{8})'
    )

    # Jina Reader API base URL
    JINA_READER_BASE = "https://r.jina.ai/"

    def __init__(
        self,
        base_url: Optional[str] = None,
        delay_seconds: Optional[float] = None,
        max_retries: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ):
        settings = get_settings()
        self.base_url = base_url or settings.bulletin_index_url
        self.delay_seconds = delay_seconds or settings.scrape_delay_seconds
        self.max_retries = max_retries or settings.max_retries
        self.timeout_seconds = timeout_seconds or settings.request_timeout_seconds
        self.jina_api_key = settings.jina_api_key
        self.url_pattern = settings.bulletin_url_pattern

        self._client: Optional[httpx.AsyncClient] = None
        self._last_request_time: float = 0

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            headers = {
                "User-Agent": "MVR-Crime-Bulletin-Scraper/1.0 (Research/Educational Use)",
            }
            # Add Jina API key if provided
            if self.jina_api_key:
                headers["Authorization"] = f"Bearer {self.jina_api_key}"
            
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_seconds),
                follow_redirects=True,
                headers=headers,
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay_seconds:
            await asyncio.sleep(self.delay_seconds - elapsed)
        self._last_request_time = time.time()

    async def _fetch_with_retry(self, url: str) -> str:
        """
        Fetch a URL using Jina Reader API with exponential backoff.

        Args:
            url: The URL to fetch

        Returns:
            The extracted text content

        Raises:
            ScraperError: If all retries fail
        """
        client = await self._get_client()
        # Jina Reader API: encode the target URL
        jina_url = f"{self.JINA_READER_BASE}{quote(url)}"
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                await self._rate_limit()
                logger.debug(f"Fetching via Jina: {url} (attempt {attempt + 1}/{self.max_retries})")
                
                response = await client.get(jina_url)
                
                # Check for Jina-specific error responses
                if response.status_code == 429:
                    logger.warning(f"Jina rate limited (attempt {attempt + 1})")
                    last_exception = JinaAPIError("Rate limited by Jina API")
                elif response.status_code != 200:
                    logger.warning(f"Jina API returned {response.status_code} for {url}")
                    last_exception = JinaAPIError(f"HTTP {response.status_code}")
                else:
                    # Jina returns content with a status prefix
                    # Format: "200 OK\nTitle: ...\n\nContent..."
                    content = response.text
                    
                    # Parse Jina's response format
                    # Format: "Title: ...\n\nURL Source: ...\n\nMarkdown Content:\n[actual content]"
                    
                    # Look for "Markdown Content:" marker
                    if 'Markdown Content:' in content:
                        # Extract content after "Markdown Content:"
                        parts = content.split('Markdown Content:', 1)
                        if len(parts) > 1:
                            return parts[1].strip()
                    
                    # Fallback: if no Markdown Content marker, check if we have actual content
                    # (Skip title and URL Source lines)
                    lines = content.split('\n')
                    content_lines = []
                    skip_next_empty = True  # Skip the first empty line after markers
                    
                    for i, line in enumerate(lines):
                        # Skip metadata lines
                        if line.startswith('Title:') or line.startswith('URL Source:'):
                            skip_next_empty = True
                            continue
                        # Skip empty lines after metadata
                        if not line.strip():
                            if skip_next_empty:
                                continue
                            skip_next_empty = False
                        else:
                            skip_next_empty = False
                            content_lines.append(line)
                    
                    extracted = '\n'.join(content_lines).strip()
                    if extracted:
                        return extracted
                    
                    # If still nothing, raise error
                    raise JinaAPIError(f"No content extracted from: {content[:200]}")

            except JinaAPIError as e:
                logger.warning(f"Jina API error for {url}: {e} (attempt {attempt + 1})")
                last_exception = e

            except httpx.RequestError as e:
                logger.warning(f"Request error for {url}: {e} (attempt {attempt + 1})")
                last_exception = e

            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}")
                last_exception = e

            # Exponential backoff
            if attempt < self.max_retries - 1:
                backoff = self.delay_seconds * (2 ** attempt)
                logger.info(f"Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)

        raise ScraperError(
            f"Failed to fetch {url} via Jina after {self.max_retries} attempts"
        ) from last_exception

    def _parse_bulletin_date_from_url(self, url: str) -> Optional[date]:
        """
        Parse the date from a bulletin URL slug.

        URL format: .../izvadok-na-del-od-dnevnite-nastani-DDMMYYYY
        Example: .../izvadok-na-del-od-dnevnite-nastani-08042026
        Returns: 2026-04-08
        """
        match = self.BULLETIN_URL_REGEX.search(url)
        if not match:
            return None

        date_str = match.group(1)  # e.g., "08042026"
        try:
            day = int(date_str[:2])
            month = int(date_str[2:4])
            year = int(date_str[4:8])
            return date(year, month, day)
        except ValueError as e:
            logger.warning(f"Could not parse date from URL {url}: {e}")
            return None

    async def _fetch_index_page_raw(self) -> str:
        """
        Fetch the raw HTML of the index page to extract bulletin links.
        Uses direct HTTP request since we need the HTML to parse links.
        """
        client = await self._get_client()
        await self._rate_limit()
        
        response = await client.get(self.base_url)
        response.raise_for_status()
        return response.text

    def _extract_bulletin_links(self, html: str) -> List[Tuple[str, date]]:
        """
        Extract all bulletin links from the index page HTML.

        Args:
            html: The HTML content of the index page

        Returns:
            List of tuples (url, publication_date)
        """
        soup = BeautifulSoup(html, "lxml")
        bulletins = []

        # Find all links matching the bulletin pattern
        for link in soup.find_all("a", href=True):
            href = link["href"]
            full_url = urljoin(self.base_url, href)

            # Check if it matches the bulletin URL pattern
            if "/izvadok-na-del-od-dnevnite-nastani-" in href:
                pub_date = self._parse_bulletin_date_from_url(full_url)
                if pub_date:
                    bulletins.append((full_url, pub_date))

        # Remove duplicates while preserving order
        seen = set()
        unique_bulletins = []
        for url, pub_date in bulletins:
            if url not in seen:
                seen.add(url)
                unique_bulletins.append((url, pub_date))

        logger.info(f"Found {len(unique_bulletins)} bulletin links on index page")
        return unique_bulletins

    def _extract_article_content(self, text: str) -> str:
        """
        Clean the extracted content from Jina.
        Jina returns markdown-like content, we clean it up.
        """
        if not text or not text.strip():
            raise ScraperError("No content extracted from bulletin")

        # Split by lines
        lines = text.split('\n')
        content_lines = []
        in_content = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Skip headers (start with #)
            if line.startswith('#'):
                continue
            
            # Skip navigation links
            if '](' in line and line.count('](') >= line.count(')'):
                continue
            
            # Skip very short lines
            if len(line) < 15:
                continue
            
            # Skip common navigation elements
            skip_patterns = [
                'Scroll to top', 'Menu toggler', 'Toggle search',
                'Со еден клик', 'Logo White', 'Image ', '![',
                'mvr.gov.mk/', 'href='
            ]
            if any(p in line for p in skip_patterns):
                continue
            
            # Skip lines that are just URLs or image references
            if line.startswith('http') or line.startswith('https'):
                continue
            if line.startswith('(') and ')' in line and len(line) < 100:
                continue
            
            # Detect actual content start
            if not in_content:
                # Look for MVR bulletin content markers
                content_markers = [
                    'Надворешната', 'ОВР', 'СВР', 'Одделот за',
                    'На ', 'полициски службеници', 'поднесе кривична',
                    'пријава против', 'лишија од слобода'
                ]
                if any(m in line for m in content_markers):
                    in_content = True
            
            if in_content or len(content_lines) > 50:  # After 50 lines we're likely in content
                content_lines.append(line)
        
        # Join with spaces (bulletin content is typically sentences)
        cleaned = ' '.join(content_lines)
        
        # Clean up any remaining markdown artifacts
        cleaned = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', cleaned)  # [text](url) -> text
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Multiple spaces to single
        
        if len(cleaned) < 100:
            raise ScraperError("No extractable content found in bulletin page")

        logger.debug(f"Extracted {len(cleaned)} characters of content")
        return cleaned

    async def get_bulletin_index(self) -> List[Tuple[str, date]]:
        """
        Fetch the bulletin index page and extract all bulletin links.

        Returns:
            List of tuples (url, publication_date) for each bulletin found
        """
        logger.info(f"Fetching bulletin index from {self.base_url}")
        
        # Fetch raw HTML to parse links
        html = await self._fetch_index_page_raw()
        return self._extract_bulletin_links(html)

    async def fetch_bulletin_content(self, url: str) -> Tuple[str, date, str]:
        """
        Fetch a single bulletin page using Jina Reader API.

        Args:
            url: The bulletin URL

        Returns:
            Tuple of (url, publication_date, raw_text_content)

        Raises:
            ScraperError: If the bulletin cannot be fetched
        """
        logger.info(f"Fetching bulletin via Jina: {url}")
        
        raw_text = await self._fetch_with_retry(url)

        pub_date = self._parse_bulletin_date_from_url(url)
        if not pub_date:
            logger.warning(f"Could not parse date from URL, using today's date")
            pub_date = date.today()

        try:
            cleaned_text = self._extract_article_content(raw_text)
        except ScraperError:
            # If cleaning fails, use raw text
            cleaned_text = raw_text
            logger.warning(f"Using raw Jina output for {url}")

        return url, pub_date, cleaned_text


# Synchronous wrapper for easier integration
def scrape_sync(url: str) -> Tuple[str, date, str]:
    """Synchronous wrapper for fetch_bulletin_content."""
    scraper = MVRScraper()

    async def _scrape():
        return await scraper.fetch_bulletin_content(url)

    try:
        return asyncio.run(_scrape())
    finally:
        pass


async def get_bulletins_async() -> List[Tuple[str, date]]:
    """Async function to get all bulletin links."""
    scraper = MVRScraper()
    try:
        return await scraper.get_bulletin_index()
    finally:
        await scraper.close()
