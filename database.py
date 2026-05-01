"""
Database models and session management for MVR Crime Bulletin scraper.
"""
import json
import logging
from datetime import datetime, date
from enum import Enum as PyEnum
from typing import Optional, List

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    Date,
    DateTime,
    ForeignKey,
    Enum,
    JSON,
    Float,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session
from sqlalchemy.dialects.sqlite import JSON as SQLiteJSON

from config import get_settings

logger = logging.getLogger(__name__)

Base = declarative_base()


class BulletinStatus(PyEnum):
    PENDING = "pending"
    PROCESSED = "processed"
    ERROR = "error"


class Bulletin(Base):
    """Represents a daily crime bulletin from MVR."""
    __tablename__ = "bulletins"

    id = Column(Integer, primary_key=True)
    url = Column(String(500), unique=True, nullable=False, index=True)
    publication_date = Column(Date, nullable=True)
    raw_text = Column(Text, nullable=True)
    processed_at = Column(DateTime, nullable=True)
    status = Column(
        Enum(BulletinStatus),
        default=BulletinStatus.PENDING,
        nullable=False
    )

    # Relationships
    crime_incidents = relationship(
        "CrimeIncident",
        back_populates="bulletin",
        cascade="all, delete-orphan"
    )
    processing_errors = relationship(
        "ProcessingError",
        back_populates="bulletin",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Bulletin(id={self.id}, url='{self.url}', status={self.status})>"


class CrimeIncident(Base):
    """Represents a single crime incident extracted from a bulletin."""
    __tablename__ = "crime_incidents"

    id = Column(Integer, primary_key=True)
    bulletin_id = Column(Integer, ForeignKey("bulletins.id"), nullable=False)
    crime_type = Column(Text, nullable=False)
    crime_date = Column(Date, nullable=True)
    location_city = Column(Text, nullable=False)
    location_address = Column(Text, nullable=True)
    # Precise coordinates from address geocoding (optional)
    precise_lat = Column(Float, nullable=True)
    precise_lon = Column(Float, nullable=True)
    perpetrator_count = Column(String(20), nullable=False)
    perpetrator_ages = Column(JSON, nullable=False, default=list)
    perpetrator_gender = Column(String(20), nullable=False)
    outcome = Column(Text, nullable=True)
    raw_text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    bulletin = relationship("Bulletin", back_populates="crime_incidents")

    def __repr__(self) -> str:
        return f"<CrimeIncident(id={self.id}, crime_type='{self.crime_type}', city='{self.location_city}')>"

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "bulletin_id": self.bulletin_id,
            "crime_type": self.crime_type,
            "crime_date": self.crime_date.isoformat() if self.crime_date else None,
            "location_city": self.location_city,
            "location_address": self.location_address,
            "precise_lat": self.precise_lat,
            "precise_lon": self.precise_lon,
            "perpetrator_count": self.perpetrator_count,
            "perpetrator_ages": self.perpetrator_ages,
            "perpetrator_gender": self.perpetrator_gender,
            "outcome": self.outcome,
            "raw_text": self.raw_text,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ProcessingError(Base):
    """Stores errors encountered during bulletin processing."""
    __tablename__ = "processing_errors"

    id = Column(Integer, primary_key=True)
    bulletin_id = Column(Integer, ForeignKey("bulletins.id"), nullable=True)
    error_type = Column(String(100), nullable=False)
    error_detail = Column(Text, nullable=True)
    raw_llm_output = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    bulletin = relationship("Bulletin", back_populates="processing_errors")

    def __repr__(self) -> str:
        return f"<ProcessingError(id={self.id}, type='{self.error_type}')>"


class DatabaseManager:
    """Manages database connections and sessions."""

    def __init__(self, database_url: Optional[str] = None):
        settings = get_settings()
        self.database_url = database_url or settings.database_url
        self.engine = create_engine(
            self.database_url,
            echo=False,
            connect_args={"check_same_thread": False} if "sqlite" in self.database_url else {},
        )
        self._session_factory = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Create all tables in the database."""
        Base.metadata.create_all(self.engine)
        logger.info("Database tables created successfully")

    def get_session(self) -> Session:
        """Create a new database session."""
        return self._session_factory()

    def close(self):
        """Close the database engine."""
        self.engine.dispose()


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """Get or create the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def init_database(database_url: Optional[str] = None) -> DatabaseManager:
    """Initialize the database manager and create tables."""
    global _db_manager
    _db_manager = DatabaseManager(database_url)
    _db_manager.create_tables()
    return _db_manager
