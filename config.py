"""
Configuration management for MVR Crime Bulletin scraper.
Loads settings from environment variables with defaults.
"""
import os
import logging
from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv

# Load .env file at module import time
load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    def __init__(self):
        # Database
        self.database_url: str = os.getenv(
            "DATABASE_URL", "sqlite:///./mvr_bulletins.db"
        )

        # Jina AI (for scraping)
        self.jina_api_key: Optional[str] = os.getenv("JINA_API_KEY")

        # OpenRouter API
        self.openrouter_api_key: Optional[str] = os.getenv("OPENROUTER_API_KEY")
        self.openrouter_base_url: str = os.getenv(
            "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"
        )

        # Optional: Anthropic API (for claude-sonnet)
        self.anthropic_api_key: Optional[str] = os.getenv("ANTHROPIC_API_KEY")

        # LLM Settings
        self.llm_model: str = os.getenv(
            "LLM_MODEL", "openrouter/auto"
        )

        # Logging
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")

        # Scraping
        self.scrape_delay_seconds: float = float(
            os.getenv("SCRAPE_DELAY_SECONDS", "2.0")
        )
        self.max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
        self.request_timeout_seconds: int = int(
            os.getenv("REQUEST_TIMEOUT_SECONDS", "30")
        )

        # Bulletin URLs
        self.bulletin_index_url: str = os.getenv(
            "BULLETIN_INDEX_URL",
            "https://mvr.gov.mk/mk-MK/odnosi-so-javnost/dnevni-bilteni"
        )
        self.bulletin_url_pattern: str = os.getenv(
            "BULLETIN_URL_PATTERN",
            "https://mvr.gov.mk/mk-MK/odnosi-so-javnost/dnevni-bilteni/izvadok-na-del-od-dnevnite-nastani-"
        )

    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []
        if not self.openrouter_api_key:
            errors.append("OPENROUTER_API_KEY is required")
        if self.scrape_delay_seconds < 0.5:
            errors.append("SCRAPE_DELAY_SECONDS should be at least 0.5 for polite scraping")
        if self.max_retries < 1:
            errors.append("MAX_RETRIES must be at least 1")
        return errors


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance (singleton pattern)."""
    return Settings()


def setup_logging(log_level: Optional[str] = None) -> None:
    """Configure structured logging."""
    settings = get_settings()
    level = log_level or settings.log_level

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
