"""Provincial corporation and non-profit data models.

This module defines the data models used by provincial ingesters,
including record structures, enums, and entity matching results.

Supports:
- Non-profit organizations (original 004 feature)
- All corporation types (005 feature extension)
"""

import hashlib
import re
from datetime import date, datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# =============================================================================
# Non-Profit Enums (004 - Legacy)
# =============================================================================


class ProvincialOrgType(str, Enum):
    """Types of provincial non-profit organizations (legacy enum from 004)."""

    SOCIETY = "society"
    AGRICULTURAL = "agricultural"
    RELIGIOUS = "religious"
    NONPROFIT_COMPANY = "nonprofit_company"
    EXTRAPROVINCIAL = "extraprovincial"
    PRIVATE_ACT = "private_act"
    UNKNOWN = "unknown"


class ProvincialOrgStatus(str, Enum):
    """Registration status of provincial non-profit organizations (legacy enum from 004)."""

    ACTIVE = "active"
    STRUCK = "struck"
    DISSOLVED = "dissolved"
    CONTINUED_OUT = "continued_out"
    AMALGAMATED = "amalgamated"
    UNKNOWN = "unknown"


# =============================================================================
# Corporation Enums (005 - Extended)
# =============================================================================


class ProvincialCorpType(str, Enum):
    """Types of provincial corporations (all types).

    Extended enumeration for all corporation types across Canadian provinces.
    Maps from province-specific terminology to standard classification.
    """

    FOR_PROFIT = "for_profit"
    NONPROFIT = "nonprofit"
    NOT_FOR_PROFIT = "not_for_profit"  # Legal distinction in ON, Federal
    COOPERATIVE = "cooperative"
    SOCIETY = "society"
    AGRICULTURAL = "agricultural"
    RELIGIOUS = "religious"
    UNLIMITED_LIABILITY = "unlimited_liability"  # BC, NS
    PROFESSIONAL = "professional"  # BC, ON, AB
    EXTRAPROVINCIAL = "extraprovincial"
    COMMUNITY_CONTRIBUTION = "community_contribution"  # BC
    BENEFIT_COMPANY = "benefit_company"  # BC
    UNKNOWN = "unknown"


class ProvincialCorpStatus(str, Enum):
    """Registration status of provincial corporations.

    Extended status enumeration for all corporation statuses.
    """

    ACTIVE = "active"
    INACTIVE = "inactive"
    DISSOLVED = "dissolved"
    STRUCK = "struck"
    AMALGAMATED = "amalgamated"
    CONTINUED_OUT = "continued_out"
    CONTINUED_IN = "continued_in"
    REVOKED = "revoked"
    SUSPENDED = "suspended"
    UNKNOWN = "unknown"


# =============================================================================
# Address and Director Models (005)
# =============================================================================


class Address(BaseModel):
    """Registered office address for corporations."""

    street_address: str | None = None
    city: str | None = None
    province: str | None = None
    postal_code: str | None = None
    country: str = "CA"

    @field_validator("postal_code")
    @classmethod
    def normalize_postal_code(cls, v: str | None) -> str | None:
        """Normalize and validate Canadian postal code format."""
        if not v:
            return None

        # Remove spaces and uppercase
        normalized = v.upper().replace(" ", "").replace("-", "")

        # Validate format: A1A1A1
        if re.match(r"^[A-Z]\d[A-Z]\d[A-Z]\d$", normalized):
            # Format as A1A 1A1
            return f"{normalized[:3]} {normalized[3:]}"

        # Invalid format, return None
        return None


class Director(BaseModel):
    """Director or officer associated with a corporation."""

    name: str
    position: str | None = None
    appointment_date: date | None = None
    resignation_date: date | None = None
    address: Address | None = None


# =============================================================================
# Provincial Data Source Model (005)
# =============================================================================


class ProvincialDataSource(BaseModel):
    """Metadata about a provincial data source."""

    province_code: str = Field(..., min_length=2, max_length=2)
    province_name: str
    source_name: str
    source_url: str
    data_format: str  # csv, xlsx, xml, json, api, search
    update_frequency: str | None = None
    bulk_available: bool = False
    enabled: bool = True
    last_ingested: datetime | None = None
    last_record_count: int | None = None


# =============================================================================
# Corporation Record Model (005)
# =============================================================================


class ProvincialCorporationRecord(BaseModel):
    """Record from provincial corporation registry.

    Represents a single corporation from any provincial data source.
    Supports all corporation types (for-profit, non-profit, cooperative, etc.).
    """

    name: str = Field(..., min_length=1, description="Legal name of corporation")
    name_french: str | None = Field(None, description="French legal name (Quebec)")
    registration_number: str = Field(..., description="Provincial registration number")
    business_number: str | None = Field(None, description="Federal Business Number (BN)")
    corp_type_raw: str = Field(..., description="Corporation type as provided by source")
    status_raw: str = Field(..., description="Status as provided by source")
    incorporation_date: date | None = Field(None, description="Date of incorporation")
    jurisdiction: str = Field(..., min_length=2, max_length=2, description="Province code")
    registered_address: Address | None = Field(None, description="Registered office")
    directors: list[Director] = Field(default_factory=list, description="Directors/officers")
    source_url: str = Field(..., description="URL of source dataset")

    @field_validator("jurisdiction")
    @classmethod
    def validate_jurisdiction(cls, v: str) -> str:
        """Validate Canadian province/territory code."""
        valid_codes = {
            "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"
        }
        v_upper = v.upper()
        if v_upper not in valid_codes:
            raise ValueError(f"Invalid jurisdiction: {v}. Must be one of {valid_codes}")
        return v_upper

    @property
    def provincial_registry_id(self) -> str:
        """Generate unique provincial registry ID.

        Format: {jurisdiction}:{registration_number}
        Example: ON:123456789
        """
        return f"{self.jurisdiction}:{self.registration_number}"

    def compute_record_hash(self) -> str:
        """Compute stable hash for change detection.

        Uses name, status, city, and postal code to detect changes.
        """
        city = self.registered_address.city if self.registered_address else ""
        postal = self.registered_address.postal_code if self.registered_address else ""
        data = f"{self.name}|{self.status_raw}|{city}|{postal}"
        return hashlib.md5(data.encode()).hexdigest()

    @property
    def corp_type_parsed(self) -> ProvincialCorpType:
        """Map raw type to standard classification (generic mapping).

        Override in province-specific subclasses for better accuracy.
        """
        raw_lower = self.corp_type_raw.lower()

        # Common mappings across provinces
        if any(kw in raw_lower for kw in ["nonprofit", "non-profit", "obnl", "sans but lucratif"]):
            return ProvincialCorpType.NONPROFIT
        elif "not-for-profit" in raw_lower or "not for profit" in raw_lower:
            return ProvincialCorpType.NOT_FOR_PROFIT
        elif any(kw in raw_lower for kw in ["cooperative", "coopérative", "co-op", "coop"]):
            return ProvincialCorpType.COOPERATIVE
        elif "society" in raw_lower or "société" in raw_lower:
            return ProvincialCorpType.SOCIETY
        elif "agricultural" in raw_lower:
            return ProvincialCorpType.AGRICULTURAL
        elif "religious" in raw_lower:
            return ProvincialCorpType.RELIGIOUS
        elif "unlimited" in raw_lower:
            return ProvincialCorpType.UNLIMITED_LIABILITY
        elif "professional" in raw_lower:
            return ProvincialCorpType.PROFESSIONAL
        elif "extraprovincial" in raw_lower or "extra-provincial" in raw_lower:
            return ProvincialCorpType.EXTRAPROVINCIAL
        elif "community contribution" in raw_lower:
            return ProvincialCorpType.COMMUNITY_CONTRIBUTION
        elif "benefit" in raw_lower:
            return ProvincialCorpType.BENEFIT_COMPANY
        elif any(kw in raw_lower for kw in ["business", "corporation", "company", "inc", "ltd"]):
            return ProvincialCorpType.FOR_PROFIT

        return ProvincialCorpType.UNKNOWN

    @property
    def status_parsed(self) -> ProvincialCorpStatus:
        """Map raw status to standard classification."""
        raw_lower = self.status_raw.lower()

        if "active" in raw_lower or "immatriculée" in raw_lower:
            return ProvincialCorpStatus.ACTIVE
        elif "inactive" in raw_lower:
            return ProvincialCorpStatus.INACTIVE
        elif "struck" in raw_lower or "cancelled" in raw_lower:
            return ProvincialCorpStatus.STRUCK
        elif "dissolved" in raw_lower or "radiée" in raw_lower:
            return ProvincialCorpStatus.DISSOLVED
        elif "amalgamated" in raw_lower or "fusionnée" in raw_lower:
            return ProvincialCorpStatus.AMALGAMATED
        elif "continued out" in raw_lower or "continuée" in raw_lower:
            return ProvincialCorpStatus.CONTINUED_OUT
        elif "continued in" in raw_lower:
            return ProvincialCorpStatus.CONTINUED_IN
        elif "revoked" in raw_lower or "révoquée" in raw_lower:
            return ProvincialCorpStatus.REVOKED
        elif "suspended" in raw_lower:
            return ProvincialCorpStatus.SUSPENDED

        return ProvincialCorpStatus.UNKNOWN


# =============================================================================
# Cross-Reference Result Model (005)
# =============================================================================


class CrossReferenceResult(BaseModel):
    """Result of cross-referencing provincial with federal/other records."""

    provincial_id: UUID
    provincial_name: str
    matched_entity_id: UUID | None = None
    matched_entity_name: str | None = None
    matched_jurisdiction: str | None = None
    match_score: float = 0.0
    match_method: str = "none"  # "business_number", "exact_name", "fuzzy_name"
    is_auto_linkable: bool = False
    requires_review: bool = False
    reviewed: bool = False
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    linked: bool = False

    @property
    def is_match(self) -> bool:
        """Check if a match was found."""
        return self.matched_entity_id is not None

    @property
    def is_high_confidence(self) -> bool:
        """Check if match is high confidence (>=95%)."""
        return self.match_score >= 0.95


# =============================================================================
# Non-Profit Record Model (004 - Legacy)
# =============================================================================


class ProvincialNonProfitRecord(BaseModel):
    """Record from a provincial non-profit registry.

    Represents a single organization from a provincial data source,
    used during parsing before entity creation.
    """

    name: str = Field(..., min_length=1, description="Organization legal name")
    org_type_raw: str = Field(..., description="Organization type as provided by source")
    status_raw: str = Field(..., description="Registration status as provided by source")
    registration_date: date | None = Field(None, description="Date of registration")
    city: str | None = Field(None, description="City location")
    postal_code: str | None = Field(None, description="Postal code (Canadian format)")
    province: str = Field(..., min_length=2, max_length=2, description="Province code (e.g., AB)")
    source_url: str = Field(..., description="URL of source dataset")

    @field_validator("postal_code")
    @classmethod
    def normalize_postal_code(cls, v: str | None) -> str | None:
        """Normalize and validate Canadian postal code format."""
        if not v:
            return None

        # Remove spaces and uppercase
        normalized = v.upper().replace(" ", "").replace("-", "")

        # Validate format: A1A1A1
        if re.match(r"^[A-Z]\d[A-Z]\d[A-Z]\d$", normalized):
            # Format as A1A 1A1
            return f"{normalized[:3]} {normalized[3:]}"

        # Invalid format, return None
        return None

    @field_validator("province")
    @classmethod
    def validate_province(cls, v: str) -> str:
        """Validate Canadian province code."""
        valid_provinces = {
            "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"
        }
        v_upper = v.upper()
        if v_upper not in valid_provinces:
            raise ValueError(f"Invalid province code: {v}. Must be one of {valid_provinces}")
        return v_upper

    def compute_record_hash(self) -> str:
        """Compute stable hash for change detection.

        Uses name, status, city, and postal code to detect changes.
        """
        data = f"{self.name}|{self.status_raw}|{self.city or ''}|{self.postal_code or ''}"
        return hashlib.md5(data.encode()).hexdigest()

    @property
    def provincial_registry_id(self) -> str:
        """Generate unique provincial registry ID.

        Format: {province}:{org_type}_{name_hash}
        Example: AB:society_a1b2c3d4
        """
        name_hash = hashlib.md5(self.name.encode()).hexdigest()[:8]
        org_type = self.org_type_parsed.value if self.org_type_parsed else "unknown"
        return f"{self.province}:{org_type}_{name_hash}"

    @property
    def org_type_parsed(self) -> ProvincialOrgType:
        """Map raw org type to standard classification."""
        raw_lower = self.org_type_raw.lower()

        mapping = {
            "societies act": ProvincialOrgType.SOCIETY,
            "society": ProvincialOrgType.SOCIETY,
            "agricultural societies act": ProvincialOrgType.AGRICULTURAL,
            "agricultural": ProvincialOrgType.AGRICULTURAL,
            "religious societies lands act": ProvincialOrgType.RELIGIOUS,
            "religious": ProvincialOrgType.RELIGIOUS,
            "companies act": ProvincialOrgType.NONPROFIT_COMPANY,
            "business corporations act": ProvincialOrgType.EXTRAPROVINCIAL,
            "extraprovincial": ProvincialOrgType.EXTRAPROVINCIAL,
            "private act": ProvincialOrgType.PRIVATE_ACT,
        }

        for key, value in mapping.items():
            if key in raw_lower:
                return value

        return ProvincialOrgType.UNKNOWN

    @property
    def status_parsed(self) -> ProvincialOrgStatus:
        """Map raw status to standard classification."""
        raw_lower = self.status_raw.lower()

        if "active" in raw_lower:
            return ProvincialOrgStatus.ACTIVE
        elif "struck" in raw_lower:
            return ProvincialOrgStatus.STRUCK
        elif "dissolved" in raw_lower:
            return ProvincialOrgStatus.DISSOLVED
        elif "continued" in raw_lower:
            return ProvincialOrgStatus.CONTINUED_OUT
        elif "amalgamated" in raw_lower:
            return ProvincialOrgStatus.AMALGAMATED

        return ProvincialOrgStatus.UNKNOWN


class EntityMatchResult(BaseModel):
    """Result of entity matching attempt.

    Records whether a provincial non-profit record matched an existing
    entity in the database, and with what confidence.
    """

    provincial_record_name: str = Field(..., description="Name from provincial record")
    matched_entity_id: UUID | None = Field(None, description="ID of matched entity if found")
    matched_entity_name: str | None = Field(None, description="Name of matched entity if found")
    match_score: float = Field(0.0, ge=0.0, le=1.0, description="Match confidence (0-1)")
    match_method: str = Field("none", description="Match method: exact, fuzzy, or none")
    requires_review: bool = Field(False, description="Whether match needs manual review")

    @property
    def is_match(self) -> bool:
        """Check if a match was found."""
        return self.matched_entity_id is not None

    @property
    def is_high_confidence(self) -> bool:
        """Check if match is high confidence (>=95%)."""
        return self.match_score >= 0.95

    @property
    def is_auto_linkable(self) -> bool:
        """Check if match can be automatically linked without review."""
        return self.is_match and self.is_high_confidence and not self.requires_review


def compute_record_hash(name: str, status: str, city: str | None, postal: str | None) -> str:
    """Compute stable hash for change detection.

    Args:
        name: Organization name
        status: Registration status
        city: City location (optional)
        postal: Postal code (optional)

    Returns:
        MD5 hash string for change detection
    """
    data = f"{name}|{status}|{city or ''}|{postal or ''}"
    return hashlib.md5(data.encode()).hexdigest()
