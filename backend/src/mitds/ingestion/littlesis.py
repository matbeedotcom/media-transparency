"""LittleSis bulk data ingester.

Ingests entity and relationship data from LittleSis bulk data exports.
LittleSis provides curated data on U.S. political and corporate power structures.

Data sources:
- Bulk data: https://littlesis.org/bulk_data
- API docs: https://littlesis.org/api

Files:
- entities.json.gz: All entities (people, organizations)
- relationships.json.gz: All relationships between entities

License: CC BY-SA 4.0
"""

import asyncio
import gzip
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, AsyncIterator
from uuid import uuid4, UUID

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session, get_redis
from ..logging import get_context_logger
from ..storage import StorageClient, get_storage
from .base import BaseIngester, IngestionConfig, IngestionResult, Neo4jHelper

logger = get_context_logger(__name__)


# =========================
# Constants
# =========================

LITTLESIS_BULK_DATA_URL = "https://littlesis.org/bulk_data"
LITTLESIS_ENTITIES_URL = "https://littlesis.org/database/public_data/entities.json.gz"
LITTLESIS_RELATIONSHIPS_URL = "https://littlesis.org/database/public_data/relationships.json.gz"

# Cache settings
CACHE_KEY_PREFIX = "littlesis"
CACHE_TTL_DAYS = 7  # Re-download bulk data weekly
CACHE_METADATA_KEY = f"{CACHE_KEY_PREFIX}:metadata"

# S3 storage keys
ENTITIES_STORAGE_KEY = "littlesis/bulk/entities.json.gz"
RELATIONSHIPS_STORAGE_KEY = "littlesis/bulk/relationships.json.gz"


# =========================
# Data Models
# =========================


class LittleSisExtension(BaseModel):
    """Extension data for entities (type-specific attributes)."""
    
    definition: str | None = None
    # Person extensions
    name_first: str | None = None
    name_last: str | None = None
    name_middle: str | None = None
    name_prefix: str | None = None
    name_suffix: str | None = None
    name_nick: str | None = None
    birthplace: str | None = None
    gender_id: int | None = None
    party_id: int | None = None
    # Org extensions
    short_name: str | None = None
    is_current: bool | None = None
    # Business extensions
    annual_profit: int | None = None
    # School extensions
    is_private: bool | None = None
    # Political fundraising extensions
    fec_id: str | None = None


class LittleSisEntity(BaseModel):
    """Entity record from LittleSis bulk data."""
    
    id: int
    name: str
    blurb: str | None = None
    summary: str | None = None
    primary_ext: str | None = None  # "Person", "Org"
    extensions: list[str] = Field(default_factory=list)  # ["Business", "Nonprofit"]
    
    # Timestamps
    created_at: str | None = None
    updated_at: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    
    # Extended data
    extension_data: dict[str, Any] = Field(default_factory=dict)
    
    # External IDs
    link: str | None = None  # LittleSis URL


class LittleSisRelationship(BaseModel):
    """Relationship record from LittleSis bulk data."""
    
    id: int
    entity1_id: int
    entity2_id: int
    category_id: int
    description1: str | None = None
    description2: str | None = None
    
    # Relationship metadata
    amount: float | None = None
    currency: str | None = None
    goods: str | None = None
    
    # Position relationships
    is_current: bool | None = None
    is_board: bool | None = None
    is_executive: bool | None = None
    is_employee: bool | None = None
    compensation: float | None = None
    
    # Ownership relationships
    percent_stake: float | None = None
    shares: int | None = None
    
    # Donation relationships
    filings: str | None = None
    
    # Timestamps
    start_date: str | None = None
    end_date: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    
    # Extended data
    notes: str | None = None


# LittleSis relationship categories
RELATIONSHIP_CATEGORIES = {
    1: "Position",      # Person holds position at organization
    2: "Education",     # Person educated at school
    3: "Membership",    # Entity is member of organization  
    4: "Family",        # Person related to person
    5: "Donation",      # Entity donates to entity
    6: "Transaction",   # Entity has transaction with entity
    7: "Lobbying",      # Entity lobbies entity
    8: "Social",        # Person has social connection to person
    9: "Professional",  # Professional service relationship
    10: "Ownership",    # Entity owns entity
    11: "Hierarchy",    # Entity has hierarchical relationship
    12: "Generic",      # Generic/other relationship
}


# =========================
# Entity Type Mapping
# =========================


def map_littlesis_entity_type(primary_ext: str | None, extensions: list[str]) -> tuple[str, str]:
    """Map LittleSis entity type to MITDS entity type.
    
    Args:
        primary_ext: Primary extension ("Person" or "Org")
        extensions: List of extension types
        
    Returns:
        Tuple of (entity_type, org_type or None)
    """
    if primary_ext == "Person":
        return "PERSON", None
    
    # Organization type mapping
    ext_set = set(e.lower() for e in extensions)
    
    if "nonprofit" in ext_set or "philanthropy" in ext_set:
        return "ORGANIZATION", "nonprofit"
    if "politicalfundraising" in ext_set or "politicalparty" in ext_set:
        return "ORGANIZATION", "political_org"
    if "governmentbody" in ext_set:
        return "ORGANIZATION", "political_org"
    if "business" in ext_set or "publiccompany" in ext_set or "privatecompany" in ext_set:
        return "ORGANIZATION", "corporation"
    if "lobbyingfirm" in ext_set:
        return "ORGANIZATION", "corporation"
    if "school" in ext_set or "university" in ext_set:
        return "ORGANIZATION", "unknown"
    if "mediaorganization" in ext_set or "newspaper" in ext_set:
        return "ORGANIZATION", "corporation"
    if "industrygroup" in ext_set or "lawfirm" in ext_set:
        return "ORGANIZATION", "corporation"
    
    # Default for organizations
    return "ORGANIZATION", "unknown"


def map_littlesis_relationship(
    category_id: int,
    rel: LittleSisRelationship,
) -> str | None:
    """Map LittleSis relationship category to MITDS relationship type.
    
    Args:
        category_id: LittleSis category ID
        rel: Full relationship record for additional context
        
    Returns:
        MITDS relationship type or None if not mappable
    """
    if category_id == 1:  # Position
        if rel.is_board:
            return "DIRECTOR_OF"
        return "EMPLOYED_BY"
    
    if category_id == 5:  # Donation
        return "FUNDED_BY"
    
    if category_id == 6:  # Transaction
        return "FUNDED_BY"
    
    if category_id == 10:  # Ownership
        return "OWNS"
    
    if category_id == 3:  # Membership
        return "EMPLOYED_BY"  # Treat membership as loose employment
    
    if category_id == 7:  # Lobbying
        return "FUNDED_BY"  # Lobbying involves payment
    
    if category_id == 11:  # Hierarchy
        return "OWNS"  # Parent-subsidiary
    
    # Categories we don't map: Education (2), Family (4), Social (8), 
    # Professional (9), Generic (12)
    return None


# =========================
# Cache Manager
# =========================


class LittleSisCacheManager:
    """Manages caching of LittleSis bulk data files."""
    
    def __init__(self):
        self._storage: StorageClient | None = None
        self._redis = None
        self._local_cache_dir = Path.home() / ".mitds" / "cache" / "littlesis"
    
    @property
    def storage(self) -> StorageClient:
        if self._storage is None:
            self._storage = get_storage()
        return self._storage
    
    async def get_redis(self):
        if self._redis is None:
            self._redis = await get_redis()
        return self._redis
    
    def _ensure_local_cache_dir(self):
        """Ensure local cache directory exists."""
        self._local_cache_dir.mkdir(parents=True, exist_ok=True)
    
    async def get_cache_metadata(self) -> dict[str, Any] | None:
        """Get cache metadata from Redis.
        
        Returns:
            Cache metadata or None if not cached
        """
        redis = await self.get_redis()
        data = await redis.get(CACHE_METADATA_KEY)
        if data:
            return json.loads(data)
        return None
    
    async def set_cache_metadata(self, metadata: dict[str, Any]):
        """Save cache metadata to Redis.
        
        Args:
            metadata: Cache metadata to store
        """
        redis = await self.get_redis()
        await redis.setex(
            CACHE_METADATA_KEY,
            CACHE_TTL_DAYS * 24 * 3600,
            json.dumps(metadata),
        )
    
    async def is_cache_valid(self) -> bool:
        """Check if the cache is still valid.
        
        Returns:
            True if cache is valid and can be used
        """
        metadata = await self.get_cache_metadata()
        if not metadata:
            return False
        
        # Check if cache has expired
        cached_at = datetime.fromisoformat(metadata.get("cached_at", "1970-01-01"))
        if datetime.utcnow() - cached_at > timedelta(days=CACHE_TTL_DAYS):
            return False
        
        return True
    
    async def download_and_cache_file(
        self,
        url: str,
        storage_key: str,
        local_filename: str,
    ) -> Path:
        """Download a file and cache it locally and in S3.
        
        Args:
            url: URL to download from
            storage_key: S3 storage key
            local_filename: Local cache filename
            
        Returns:
            Path to local cached file
        """
        self._ensure_local_cache_dir()
        local_path = self._local_cache_dir / local_filename
        
        logger.info(f"Downloading {url}...")
        
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            data = response.content
        
        # Save to local cache
        local_path.write_bytes(data)
        logger.info(f"Cached locally: {local_path} ({len(data) / 1024 / 1024:.1f} MB)")
        
        # Save to S3
        try:
            self.storage.upload_file(
                data,
                storage_key,
                content_type="application/gzip",
                metadata={"source_url": url},
            )
            logger.info(f"Cached to S3: {storage_key}")
        except Exception as e:
            logger.warning(f"Failed to cache to S3: {e}")
        
        return local_path
    
    async def get_cached_file(
        self,
        url: str,
        storage_key: str,
        local_filename: str,
        force_refresh: bool = False,
    ) -> Path:
        """Get a cached file, downloading if necessary.
        
        Args:
            url: URL to download from
            storage_key: S3 storage key
            local_filename: Local cache filename
            force_refresh: Force re-download even if cached
            
        Returns:
            Path to local cached file
        """
        self._ensure_local_cache_dir()
        local_path = self._local_cache_dir / local_filename
        
        # Check if we have a valid local cache
        if not force_refresh and local_path.exists():
            cache_valid = await self.is_cache_valid()
            if cache_valid:
                logger.info(f"Using local cache: {local_path}")
                return local_path
        
        # Try to get from S3 if local doesn't exist
        if not force_refresh:
            try:
                if self.storage.file_exists(storage_key):
                    logger.info(f"Downloading from S3 cache: {storage_key}")
                    data = self.storage.download_file(storage_key)
                    local_path.write_bytes(data)
                    return local_path
            except Exception as e:
                logger.warning(f"Failed to get from S3 cache: {e}")
        
        # Download fresh
        return await self.download_and_cache_file(url, storage_key, local_filename)
    
    async def get_entities_file(self, force_refresh: bool = False) -> Path:
        """Get the entities bulk data file.
        
        Args:
            force_refresh: Force re-download
            
        Returns:
            Path to local entities file
        """
        return await self.get_cached_file(
            LITTLESIS_ENTITIES_URL,
            ENTITIES_STORAGE_KEY,
            "entities.json.gz",
            force_refresh=force_refresh,
        )
    
    async def get_relationships_file(self, force_refresh: bool = False) -> Path:
        """Get the relationships bulk data file.
        
        Args:
            force_refresh: Force re-download
            
        Returns:
            Path to local relationships file
        """
        return await self.get_cached_file(
            LITTLESIS_RELATIONSHIPS_URL,
            RELATIONSHIPS_STORAGE_KEY,
            "relationships.json.gz",
            force_refresh=force_refresh,
        )


# =========================
# Data Parser
# =========================


class LittleSisParser:
    """Parser for LittleSis bulk data."""
    
    def __init__(self):
        self.entity_id_map: dict[int, UUID] = {}  # LittleSis ID -> MITDS UUID
    
    def iter_entities(self, file_path: Path) -> AsyncIterator[LittleSisEntity]:
        """Iterate over entities in the bulk file.
        
        Args:
            file_path: Path to entities.json.gz
            
        Yields:
            Parsed entity records
        """
        # #region agent log
        import json as _json
        _log_path = r"c:\Users\mail\dev\personal\media_transparency\.cursor\debug.log"
        with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H1","location":"littlesis.py:iter_entities","message":"Starting entity iteration","data":{"file_path":str(file_path)},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
        # #endregion
        
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            # #region agent log
            first_char = f.read(1)
            with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H1","location":"littlesis.py:iter_entities","message":"First character of file","data":{"first_char":first_char,"is_array":first_char=="["},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
            f.seek(0)  # Reset to beginning
            # #endregion
            
            # Check if file is JSON array (starts with '[') or JSONL
            content_start = f.read(1)
            f.seek(0)
            
            if content_start == '[':
                # #region agent log
                with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H1","location":"littlesis.py:iter_entities","message":"Detected JSON array format, parsing full file","timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
                # #endregion
                
                # JSON array format - parse entire file
                try:
                    data = json.load(f)
                    # #region agent log
                    with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H1","location":"littlesis.py:iter_entities","message":"Parsed JSON array","data":{"num_items":len(data),"first_item_keys":list(data[0].keys()) if data else []},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
                    # #endregion
                    
                    for i, item in enumerate(data):
                        try:
                            # JSON:API format: data is under 'attributes'
                            if 'attributes' in item:
                                # #region agent log
                                if i == 0:
                                    with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H2","location":"littlesis.py:iter_entities","message":"JSON:API format detected","data":{"attr_keys":list(item['attributes'].keys())[:10]},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
                                # #endregion
                                entity_data = item['attributes']
                                entity_data['link'] = item.get('links', {}).get('self')
                            else:
                                entity_data = item
                            
                            # Map field names from LittleSis format to our model
                            mapped_data = self._map_entity_fields(entity_data)
                            
                            yield LittleSisEntity(**mapped_data)
                        except Exception as e:
                            # #region agent log
                            if i < 5:
                                with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H3","location":"littlesis.py:iter_entities","message":"Entity parse error","data":{"index":i,"error":str(e),"item_keys":list(item.keys()) if isinstance(item,dict) else None},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
                            # #endregion
                            logger.warning(f"Failed to parse entity {i}: {e}")
                            continue
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON array: {e}")
                    return
            else:
                # JSONL format - parse line by line
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        yield LittleSisEntity(**data)
                    except (json.JSONDecodeError, Exception) as e:
                        logger.warning(f"Failed to parse entity: {e}")
                        continue
    
    def _map_entity_fields(self, data: dict) -> dict:
        """Map LittleSis field names to our model fields.
        
        Args:
            data: Raw entity data from LittleSis
            
        Returns:
            Mapped data matching LittleSisEntity model
        """
        # LittleSis uses 'types' array, we use 'extensions'
        # LittleSis uses 'primary_ext', we use 'primary_ext' (same)
        extensions_data = data.get('extensions', {})
        
        return {
            'id': data.get('id'),
            'name': data.get('name'),
            'blurb': data.get('blurb'),
            'summary': data.get('summary'),
            'primary_ext': data.get('primary_ext'),
            'extensions': data.get('types', []),  # 'types' in LittleSis -> 'extensions' in our model
            'created_at': data.get('created_at'),
            'updated_at': data.get('updated_at'),
            'start_date': data.get('start_date'),
            'end_date': data.get('end_date'),
            'extension_data': extensions_data,  # Nested extension details
            'link': data.get('link'),
        }
    
    def iter_relationships(self, file_path: Path) -> AsyncIterator[LittleSisRelationship]:
        """Iterate over relationships in the bulk file.
        
        Args:
            file_path: Path to relationships.json.gz
            
        Yields:
            Parsed relationship records
        """
        # #region agent log
        import json as _json
        _log_path = r"c:\Users\mail\dev\personal\media_transparency\.cursor\debug.log"
        with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H1","location":"littlesis.py:iter_relationships","message":"Starting relationship iteration","data":{"file_path":str(file_path)},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
        # #endregion
        
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            # Check if file is JSON array or JSONL
            content_start = f.read(1)
            f.seek(0)
            
            if content_start == '[':
                # #region agent log
                with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H1","location":"littlesis.py:iter_relationships","message":"Detected JSON array format","timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
                # #endregion
                
                # JSON array format
                try:
                    data = json.load(f)
                    # #region agent log
                    with open(_log_path, "a") as _lf: _lf.write(_json.dumps({"hypothesisId":"H1","location":"littlesis.py:iter_relationships","message":"Parsed relationships JSON array","data":{"num_items":len(data)},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session"})+"\n")
                    # #endregion
                    
                    for i, item in enumerate(data):
                        try:
                            # JSON:API format
                            if 'attributes' in item:
                                rel_data = item['attributes']
                            else:
                                rel_data = item
                            
                            mapped_data = self._map_relationship_fields(rel_data)
                            yield LittleSisRelationship(**mapped_data)
                        except Exception as e:
                            if i < 5:
                                logger.warning(f"Failed to parse relationship {i}: {e}")
                            continue
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse relationships JSON: {e}")
                    return
            else:
                # JSONL format
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        yield LittleSisRelationship(**data)
                    except (json.JSONDecodeError, Exception) as e:
                        logger.warning(f"Failed to parse relationship: {e}")
                        continue
    
    def _map_relationship_fields(self, data: dict) -> dict:
        """Map LittleSis relationship field names to our model.
        
        Args:
            data: Raw relationship data from LittleSis
            
        Returns:
            Mapped data matching LittleSisRelationship model
        """
        return {
            'id': data.get('id'),
            'entity1_id': data.get('entity1_id'),
            'entity2_id': data.get('entity2_id'),
            'category_id': data.get('category_id'),
            'description1': data.get('description1'),
            'description2': data.get('description2'),
            'amount': data.get('amount'),
            'currency': data.get('currency'),
            'goods': data.get('goods'),
            'is_current': data.get('is_current'),
            'is_board': data.get('is_board'),
            'is_executive': data.get('is_executive'),
            'is_employee': data.get('is_employee'),
            'compensation': data.get('compensation'),
            'percent_stake': data.get('percent_stake'),
            'shares': data.get('shares'),
            'filings': data.get('filings'),
            'start_date': data.get('start_date'),
            'end_date': data.get('end_date'),
            'created_at': data.get('created_at'),
            'updated_at': data.get('updated_at'),
            'notes': data.get('notes'),
        }
    
    def parse_date(self, date_str: str | None) -> date | None:
        """Parse a date string from LittleSis.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            Parsed date or None
        """
        if not date_str:
            return None
        
        # Try various formats
        for fmt in ["%Y-%m-%d", "%Y-%m", "%Y"]:
            try:
                dt = datetime.strptime(date_str[:len("YYYY-MM-DD")], fmt)
                return dt.date()
            except ValueError:
                continue
        
        return None
    
    def entity_to_dict(self, entity: LittleSisEntity) -> dict[str, Any]:
        """Convert LittleSis entity to MITDS entity format.
        
        Args:
            entity: LittleSis entity
            
        Returns:
            Dictionary for database insertion
        """
        entity_type, org_type = map_littlesis_entity_type(
            entity.primary_ext, 
            entity.extensions,
        )
        
        # Extract aliases from extension data
        aliases = []
        ext_data = entity.extension_data or {}
        if ext_data.get("short_name"):
            aliases.append(ext_data["short_name"])
        if ext_data.get("name_nick"):
            aliases.append(ext_data["name_nick"])
        
        return {
            "entity_type": entity_type,
            "name": entity.name,
            "aliases": aliases,
            "org_type": org_type,
            "status": "active",
            "external_ids": {
                "littlesis_id": str(entity.id),
                "littlesis_url": entity.link or f"https://littlesis.org/entities/{entity.id}",
            },
            "metadata": {
                "blurb": entity.blurb,
                "summary": entity.summary,
                "primary_ext": entity.primary_ext,
                "extensions": entity.extensions,
                "extension_data": entity.extension_data,
            },
            "start_date": self.parse_date(entity.start_date),
            "end_date": self.parse_date(entity.end_date),
        }
    
    def relationship_to_dict(
        self,
        rel: LittleSisRelationship,
        source_uuid: UUID,
        target_uuid: UUID,
    ) -> dict[str, Any] | None:
        """Convert LittleSis relationship to MITDS format.
        
        Args:
            rel: LittleSis relationship
            source_uuid: MITDS UUID for source entity
            target_uuid: MITDS UUID for target entity
            
        Returns:
            Dictionary for database insertion, or None if not mappable
        """
        rel_type = map_littlesis_relationship(rel.category_id, rel)
        if not rel_type:
            return None
        
        # Build properties based on relationship type
        properties = {
            "littlesis_id": rel.id,
            "littlesis_category": RELATIONSHIP_CATEGORIES.get(rel.category_id, "unknown"),
            "description1": rel.description1,
            "description2": rel.description2,
            "notes": rel.notes,
        }
        
        if rel.amount:
            properties["amount"] = rel.amount
            properties["currency"] = rel.currency or "USD"
        
        if rel.compensation:
            properties["compensation"] = rel.compensation
        
        if rel.percent_stake:
            properties["ownership_percentage"] = rel.percent_stake
        
        if rel.is_board is not None:
            properties["is_board"] = rel.is_board
        if rel.is_executive is not None:
            properties["is_executive"] = rel.is_executive
        
        return {
            "rel_type": rel_type,
            "source_entity_id": str(source_uuid),
            "target_entity_id": str(target_uuid),
            "valid_from": self.parse_date(rel.start_date),
            "valid_to": self.parse_date(rel.end_date),
            "is_current": rel.is_current if rel.is_current is not None else True,
            "confidence": 0.9,  # LittleSis data is human-curated
            "properties": properties,
        }


# =========================
# Ingester
# =========================


class LittleSisIngester(BaseIngester[LittleSisEntity]):
    """Ingester for LittleSis bulk data.
    
    Downloads and processes the LittleSis entities and relationships
    bulk data exports.
    """
    
    def __init__(self):
        super().__init__("littlesis")
        self.cache_manager = LittleSisCacheManager()
        self.parser = LittleSisParser()
    
    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[LittleSisEntity]:
        """Fetch entity records from bulk data.
        
        Args:
            config: Ingestion configuration
            
        Yields:
            Entity records
        """
        force_refresh = config.extra_params.get("force_refresh", False)
        
        # Download/get cached entities file
        entities_path = await self.cache_manager.get_entities_file(
            force_refresh=force_refresh
        )
        
        # Iterate over entities
        for entity in self.parser.iter_entities(entities_path):
            yield entity
    
    async def process_record(self, record: LittleSisEntity) -> dict[str, Any]:
        """Process a single entity record.
        
        Args:
            record: Entity to process
            
        Returns:
            Processing result
        """
        entity_data = self.parser.entity_to_dict(record)
        result = {}
        entity_id = None
        is_new = False
        
        # --- PostgreSQL: Create/update entity ---
        async with get_db_session() as db:
            # Check for existing entity by LittleSis ID
            check_query = text("""
                SELECT id FROM entities
                WHERE external_ids->>'littlesis_id' = :littlesis_id
            """)
            db_result = await db.execute(
                check_query,
                {"littlesis_id": str(record.id)},
            )
            existing = db_result.fetchone()
            
            if existing:
                entity_id = existing.id
                # Update existing
                update_query = text("""
                    UPDATE entities SET
                        name = :name,
                        metadata = :metadata,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                await db.execute(
                    update_query,
                    {
                        "id": entity_id,
                        "name": entity_data["name"],
                        "metadata": json.dumps(entity_data.get("metadata", {})),
                        "updated_at": datetime.utcnow(),
                    },
                )
                # Note: db.commit() is handled automatically by get_db_session() context manager

                # Track mapping
                self.parser.entity_id_map[record.id] = entity_id
                result = {"updated": True, "entity_id": str(entity_id)}
            else:
                # Create new entity
                entity_id = uuid4()
                is_new = True
                insert_query = text("""
                    INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                    VALUES (:id, :name, :entity_type, CAST(:external_ids AS jsonb),
                            CAST(:metadata AS jsonb), NOW())
                """)
                
                await db.execute(
                    insert_query,
                    {
                        "id": entity_id,
                        "name": entity_data["name"],
                        "entity_type": entity_data["entity_type"],
                        "external_ids": json.dumps(entity_data.get("external_ids", {})),
                        "metadata": json.dumps(entity_data.get("metadata", {})),
                    },
                )
                await db.commit()

                # Track mapping
                self.parser.entity_id_map[record.id] = entity_id
                result = {"created": True, "entity_id": str(entity_id)}
        
        # --- Neo4j: Create/update graph node ---
        try:
            await self._sync_entity_to_neo4j(record, entity_data, entity_id)
        except Exception as e:
            self.logger.warning(f"Neo4j sync failed for entity {record.id}: {e}")
        
        return result
    
    async def _sync_entity_to_neo4j(
        self,
        record: LittleSisEntity,
        entity_data: dict[str, Any],
        entity_id: UUID,
    ):
        """Sync an entity to Neo4j graph database.
        
        Args:
            record: Original LittleSis record
            entity_data: Parsed entity data
            entity_id: MITDS entity UUID
        """
        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()
            
            # Determine node label based on entity type
            if entity_data["entity_type"] == "PERSON":
                label = "Person"
                props = {
                    "id": str(entity_id),
                    "name": entity_data["name"],
                    "entity_type": "PERSON",
                    "littlesis_id": str(record.id),
                    "littlesis_url": entity_data["external_ids"].get("littlesis_url"),
                    "blurb": entity_data["metadata"].get("blurb"),
                    "source": "littlesis",
                    "updated_at": now,
                }
                
                # Add person-specific fields from extension data
                ext_data = record.extension_data or {}
                if ext_data.get("name_first"):
                    props["first_name"] = ext_data["name_first"]
                if ext_data.get("name_last"):
                    props["last_name"] = ext_data["name_last"]
                
                await session.run(
                    """
                    MERGE (p:Person {littlesis_id: $littlesis_id})
                    ON CREATE SET p += $props
                    ON MATCH SET p.name = $props.name,
                                 p.blurb = $props.blurb,
                                 p.updated_at = $props.updated_at
                    """,
                    littlesis_id=str(record.id),
                    props=props,
                )
            else:
                label = "Organization"
                props = {
                    "id": str(entity_id),
                    "name": entity_data["name"],
                    "entity_type": "ORGANIZATION",
                    "org_type": entity_data.get("org_type"),
                    "littlesis_id": str(record.id),
                    "littlesis_url": entity_data["external_ids"].get("littlesis_url"),
                    "blurb": entity_data["metadata"].get("blurb"),
                    "extensions": entity_data["metadata"].get("extensions", []),
                    "source": "littlesis",
                    "updated_at": now,
                }
                
                await session.run(
                    """
                    MERGE (o:Organization {littlesis_id: $littlesis_id})
                    ON CREATE SET o += $props
                    ON MATCH SET o.name = $props.name,
                                 o.org_type = $props.org_type,
                                 o.blurb = $props.blurb,
                                 o.updated_at = $props.updated_at
                    """,
                    littlesis_id=str(record.id),
                    props=props,
                )
    
    async def _sync_relationship_to_neo4j(
        self,
        rel: LittleSisRelationship,
        rel_data: dict[str, Any],
    ):
        """Sync a relationship to Neo4j graph database.
        
        Args:
            rel: Original LittleSis relationship
            rel_data: Parsed relationship data
        """
        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()
            
            # Get LittleSis IDs for the entities
            source_ls_id = str(rel.entity1_id)
            target_ls_id = str(rel.entity2_id)
            
            # Map MITDS relationship types to Neo4j relationship types
            rel_type = rel_data["rel_type"]
            
            # Build relationship properties
            props = {
                "littlesis_id": str(rel.id),
                "littlesis_category": rel_data["properties"].get("littlesis_category"),
                "confidence": rel_data["confidence"],
                "source": "littlesis",
                "is_current": rel_data["is_current"],
                "updated_at": now,
            }
            
            if rel_data.get("valid_from"):
                props["start_date"] = rel_data["valid_from"].isoformat()
            if rel_data.get("valid_to"):
                props["end_date"] = rel_data["valid_to"].isoformat()
            
            # Add type-specific properties
            if rel_data["properties"].get("amount"):
                props["amount"] = rel_data["properties"]["amount"]
                props["currency"] = rel_data["properties"].get("currency", "USD")
            if rel_data["properties"].get("compensation"):
                props["compensation"] = rel_data["properties"]["compensation"]
            if rel_data["properties"].get("ownership_percentage"):
                props["ownership_percentage"] = rel_data["properties"]["ownership_percentage"]
            if rel_data["properties"].get("is_board"):
                props["is_board"] = rel_data["properties"]["is_board"]
            if rel_data["properties"].get("description1"):
                props["description"] = rel_data["properties"]["description1"]
            
            # Create relationship based on type
            # We need to handle both Person and Organization nodes
            if rel_type == "DIRECTOR_OF":
                # Person -> Organization
                await session.run(
                    """
                    MATCH (p:Person {littlesis_id: $source_id})
                    MATCH (o:Organization {littlesis_id: $target_id})
                    MERGE (p)-[r:DIRECTOR_OF]->(o)
                    SET r += $props
                    """,
                    source_id=source_ls_id,
                    target_id=target_ls_id,
                    props=props,
                )
            elif rel_type == "EMPLOYED_BY":
                # Person -> Organization
                await session.run(
                    """
                    MATCH (p:Person {littlesis_id: $source_id})
                    MATCH (o:Organization {littlesis_id: $target_id})
                    MERGE (p)-[r:EMPLOYED_BY]->(o)
                    SET r += $props
                    """,
                    source_id=source_ls_id,
                    target_id=target_ls_id,
                    props=props,
                )
            elif rel_type == "FUNDED_BY":
                # Entity -> Entity (donations, transactions)
                # Try Organization first, fall back to Person
                await session.run(
                    """
                    OPTIONAL MATCH (s1:Organization {littlesis_id: $source_id})
                    OPTIONAL MATCH (s2:Person {littlesis_id: $source_id})
                    OPTIONAL MATCH (t1:Organization {littlesis_id: $target_id})
                    OPTIONAL MATCH (t2:Person {littlesis_id: $target_id})
                    WITH COALESCE(s1, s2) AS source, COALESCE(t1, t2) AS target
                    WHERE source IS NOT NULL AND target IS NOT NULL
                    MERGE (source)-[r:FUNDED_BY]->(target)
                    SET r += $props
                    """,
                    source_id=source_ls_id,
                    target_id=target_ls_id,
                    props=props,
                )
            elif rel_type == "OWNS":
                # Organization -> Organization (ownership, hierarchy)
                await session.run(
                    """
                    MATCH (o1:Organization {littlesis_id: $source_id})
                    MATCH (o2:Organization {littlesis_id: $target_id})
                    MERGE (o1)-[r:OWNS]->(o2)
                    SET r += $props
                    """,
                    source_id=source_ls_id,
                    target_id=target_ls_id,
                    props=props,
                )
    
    async def ingest_relationships(
        self,
        config: IngestionConfig | None = None,
    ) -> IngestionResult:
        """Ingest relationships after entities are loaded.
        
        Args:
            config: Ingestion configuration
            
        Returns:
            Ingestion result
        """
        if config is None:
            config = IngestionConfig()
        
        result = IngestionResult(
            run_id=uuid4(),
            source=f"{self.source_name}_relationships",
            status="running",
            started_at=datetime.utcnow(),
        )
        
        force_refresh = config.extra_params.get("force_refresh", False)
        
        try:
            # Get relationships file
            relationships_path = await self.cache_manager.get_relationships_file(
                force_refresh=force_refresh
            )
            
            # Build entity ID map if not already populated
            if not self.parser.entity_id_map:
                await self._load_entity_id_map()
            
            async with get_db_session() as db:
                for rel in self.parser.iter_relationships(relationships_path):
                    result.records_processed += 1
                    
                    try:
                        # Get MITDS UUIDs for entities
                        source_uuid = self.parser.entity_id_map.get(rel.entity1_id)
                        target_uuid = self.parser.entity_id_map.get(rel.entity2_id)
                        
                        if not source_uuid or not target_uuid:
                            # Entities not yet imported
                            continue
                        
                        # Convert relationship
                        rel_data = self.parser.relationship_to_dict(
                            rel, source_uuid, target_uuid
                        )
                        
                        if not rel_data:
                            # Relationship type not mappable
                            continue
                        
                        # Check for existing relationship
                        check_query = text("""
                            SELECT id FROM relationships
                            WHERE properties->>'littlesis_id' = :littlesis_id
                        """)
                        existing = await db.execute(
                            check_query,
                            {"littlesis_id": str(rel.id)},
                        )
                        
                        if existing.fetchone():
                            result.duplicates_found += 1
                            continue
                        
                        # Insert relationship
                        insert_query = text("""
                            INSERT INTO relationships (
                                id, relationship_type, from_entity_id, to_entity_id,
                                start_date, end_date, is_current, confidence,
                                properties, created_at
                            ) VALUES (
                                :id, :rel_type, :from_id, :to_id,
                                :start_date, :end_date, :is_current, :confidence,
                                :properties, :created_at
                            )
                        """)
                        
                        await db.execute(
                            insert_query,
                            {
                                "id": uuid4(),
                                "rel_type": rel_data["rel_type"],
                                "from_id": rel_data["source_entity_id"],
                                "to_id": rel_data["target_entity_id"],
                                "start_date": rel_data["valid_from"],
                                "end_date": rel_data["valid_to"],
                                "is_current": rel_data["is_current"],
                                "confidence": rel_data["confidence"],
                                "properties": json.dumps(rel_data["properties"]),
                                "created_at": datetime.utcnow(),
                            },
                        )
                        
                        # Sync to Neo4j
                        try:
                            await self._sync_relationship_to_neo4j(rel, rel_data)
                        except Exception as neo4j_err:
                            self.logger.warning(
                                f"Neo4j sync failed for relationship {rel.id}: {neo4j_err}"
                            )
                        
                        result.records_created += 1
                        
                        # Progress logging
                        if result.records_processed % 1000 == 0:
                            self.logger.info(
                                f"Relationships progress: {result.records_processed} processed, "
                                f"{result.records_created} created"
                            )
                        
                        # Check limit
                        if config.limit and result.records_created >= config.limit:
                            break
                    
                    except Exception as e:
                        result.errors.append({
                            "relationship_id": rel.id,
                            "error": str(e),
                        })
                        continue

                # Note: db.commit() is handled automatically by get_db_session() context manager
            
            result.status = "completed" if not result.errors else "partial"
            result.completed_at = datetime.utcnow()
            
        except Exception as e:
            result.status = "failed"
            result.completed_at = datetime.utcnow()
            result.errors.append({"error": str(e), "fatal": True})
            self.logger.exception("Relationship ingestion failed")
        
        return result
    
    async def _load_entity_id_map(self):
        """Load entity ID mapping from database."""
        async with get_db_session() as db:
            query = text("""
                SELECT id, external_ids->>'littlesis_id' as littlesis_id
                FROM entities
                WHERE external_ids->>'littlesis_id' IS NOT NULL
            """)
            result = await db.execute(query)
            
            for row in result.fetchall():
                ls_id = int(row.littlesis_id)
                self.parser.entity_id_map[ls_id] = row.id
            
            self.logger.info(f"Loaded {len(self.parser.entity_id_map)} entity ID mappings")
    
    async def get_last_sync_time(self) -> datetime | None:
        """Get the last successful sync time."""
        redis = await self.cache_manager.get_redis()
        timestamp = await redis.get(f"{CACHE_KEY_PREFIX}:last_sync")
        if timestamp:
            return datetime.fromisoformat(timestamp.decode())
        return None
    
    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save the sync timestamp."""
        redis = await self.cache_manager.get_redis()
        await redis.set(
            f"{CACHE_KEY_PREFIX}:last_sync",
            timestamp.isoformat(),
        )
        
        # Also update cache metadata
        await self.cache_manager.set_cache_metadata({
            "cached_at": datetime.utcnow().isoformat(),
            "last_sync": timestamp.isoformat(),
        })


# =========================
# Convenience Functions
# =========================


async def run_littlesis_ingestion(
    entities: bool = True,
    relationships: bool = True,
    force_refresh: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run LittleSis bulk data ingestion.
    
    Args:
        entities: Whether to ingest entities
        relationships: Whether to ingest relationships
        force_refresh: Force re-download of bulk data
        limit: Maximum records to process
        
    Returns:
        Combined ingestion results
    """
    ingester = LittleSisIngester()
    
    results = {
        "entities": None,
        "relationships": None,
    }
    
    config = IngestionConfig(
        incremental=False,  # Bulk import is always full
        limit=limit,
        extra_params={"force_refresh": force_refresh},
    )
    
    if entities:
        entity_result = await ingester.run(config)
        results["entities"] = {
            "status": entity_result.status,
            "records_processed": entity_result.records_processed,
            "records_created": entity_result.records_created,
            "records_updated": entity_result.records_updated,
            "duplicates_found": entity_result.duplicates_found,
            "errors": len(entity_result.errors),
            "duration_seconds": entity_result.duration_seconds,
        }
    
    if relationships:
        rel_result = await ingester.ingest_relationships(config)
        results["relationships"] = {
            "status": rel_result.status,
            "records_processed": rel_result.records_processed,
            "records_created": rel_result.records_created,
            "duplicates_found": rel_result.duplicates_found,
            "errors": len(rel_result.errors),
            "duration_seconds": rel_result.duration_seconds,
        }
    
    return results


async def get_littlesis_stats() -> dict[str, Any]:
    """Get statistics about cached LittleSis data.
    
    Returns:
        Cache and data statistics
    """
    cache_manager = LittleSisCacheManager()
    
    stats = {
        "cache_valid": await cache_manager.is_cache_valid(),
        "cache_metadata": await cache_manager.get_cache_metadata(),
    }
    
    # Count entities and relationships from database
    async with get_db_session() as db:
        entity_count = await db.execute(text("""
            SELECT COUNT(*) FROM entities
            WHERE external_ids->>'littlesis_id' IS NOT NULL
        """))
        stats["entity_count"] = entity_count.scalar()
        
        rel_count = await db.execute(text("""
            SELECT COUNT(*) FROM relationships
            WHERE properties->>'littlesis_id' IS NOT NULL
        """))
        stats["relationship_count"] = rel_count.scalar()
    
    return stats
