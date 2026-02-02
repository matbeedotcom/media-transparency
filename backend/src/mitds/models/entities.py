"""Entity models for MITDS.

This module defines the core domain entities:
- Organization: Legal entities (companies, nonprofits, foundations)
- Person: Individuals with potential influence
- Outlet: Media publications or broadcast operations
- Sponsor: Entities providing financial support

See data-model.md for complete schema documentation.
"""

from datetime import date, datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .base import Address, EntityBase, EntityType, RoleAssignment


# =============================================================================
# Organization
# =============================================================================


class OrgType(str, Enum):
    """Types of organizations tracked in MITDS."""

    NONPROFIT = "nonprofit"
    CORPORATION = "corporation"
    FOUNDATION = "foundation"
    POLITICAL_ORG = "political_org"
    UNKNOWN = "unknown"


class OrgStatus(str, Enum):
    """Operational status of an organization."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    REVOKED = "revoked"
    UNKNOWN = "unknown"


class Organization(EntityBase):
    """Legal entities (companies, nonprofits, foundations, political organizations).

    Organizations are the primary entities for tracking funding relationships
    and ownership structures.
    """

    entity_type: EntityType = Field(default=EntityType.ORGANIZATION, frozen=True)

    # Identifying attributes
    name: str = Field(..., min_length=2, description="Canonical legal name")
    aliases: list[str] = Field(default_factory=list, description="DBA names, former names")
    org_type: OrgType = Field(default=OrgType.UNKNOWN, description="Organization classification")

    # Jurisdiction
    jurisdiction: str = Field(
        default="US",
        min_length=2,
        max_length=5,
        description="Country code (US, CA) or state/province",
    )
    registration_id: str | None = Field(
        default=None, description="EIN (US), BN (CA), company number"
    )

    # Source identifiers
    ein: str | None = Field(default=None, description="US Employer Identification Number")
    bn: str | None = Field(default=None, description="Canadian Business Number")
    sedar_profile: str | None = Field(default=None, description="SEDAR+ profile identifier for Canadian public companies")
    opencorp_id: str | None = Field(default=None, description="OpenCorporates company URL")

    # Metadata
    incorporation_date: date | None = Field(default=None, description="Date of incorporation")
    status: OrgStatus = Field(default=OrgStatus.UNKNOWN, description="Operational status")
    address: Address | None = Field(default=None, description="Primary address")

    @field_validator("ein")
    @classmethod
    def validate_ein(cls, v: str | None) -> str | None:
        """Validate US EIN format: XX-XXXXXXX."""
        if v is None:
            return None
        import re

        if not re.match(r"^\d{2}-\d{7}$", v):
            raise ValueError("EIN must match format XX-XXXXXXX")
        return v

    @field_validator("bn")
    @classmethod
    def validate_bn(cls, v: str | None) -> str | None:
        """Validate Canadian BN format: 9 digits + RR + 4 digits."""
        if v is None:
            return None
        import re

        if not re.match(r"^\d{9}RR\d{4}$", v):
            raise ValueError("BN must match format 123456789RR0001")
        return v


# =============================================================================
# Person
# =============================================================================


class Person(EntityBase):
    """Individuals with potential influence (journalists, executives, board members, donors).

    Persons are linked to organizations through role assignments and can
    serve as board members, employees, or donors.
    """

    entity_type: EntityType = Field(default=EntityType.PERSON, frozen=True)

    # Identifying attributes
    name: str = Field(..., min_length=2, description="Canonical name")
    aliases: list[str] = Field(default_factory=list, description="Known alternate names")

    # Source identifiers (when available)
    irs_990_name: str | None = Field(
        default=None, description="Name as appears in IRS 990"
    )
    opencorp_officer_id: str | None = Field(
        default=None, description="OpenCorporates officer ID"
    )

    # Demographics (when disclosed)
    location: str | None = Field(default=None, description="City, State/Province")

    # Derived
    roles: list[RoleAssignment] = Field(
        default_factory=list, description="Time-bounded role assignments"
    )

    def add_role(
        self,
        organization_id: UUID,
        title: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        compensation: float | None = None,
        hours_per_week: float | None = None,
    ) -> None:
        """Add a role assignment for this person."""
        role = RoleAssignment(
            organization_id=organization_id,
            title=title,
            start_date=start_date,
            end_date=end_date,
            compensation=compensation,
            hours_per_week=hours_per_week,
        )
        self.roles.append(role)

    def get_current_roles(self) -> list[RoleAssignment]:
        """Get all currently active roles."""
        return [r for r in self.roles if r.is_current]

    def get_roles_at(self, point_in_time: datetime) -> list[RoleAssignment]:
        """Get all roles active at a specific point in time."""
        result = []
        for role in self.roles:
            started = role.start_date is None or role.start_date <= point_in_time
            not_ended = role.end_date is None or role.end_date > point_in_time
            if started and not_ended:
                result.append(role)
        return result


# =============================================================================
# Outlet
# =============================================================================


class MediaType(str, Enum):
    """Types of media outlets."""

    DIGITAL = "digital"
    PRINT = "print"
    BROADCAST = "broadcast"
    PODCAST = "podcast"
    HYBRID = "hybrid"


class Outlet(EntityBase):
    """Media publications or broadcast operations.

    Outlets are the primary entities for tracking media influence,
    including their funding sources and infrastructure sharing.
    """

    entity_type: EntityType = Field(default=EntityType.OUTLET, frozen=True)

    # Identifying attributes
    name: str = Field(..., min_length=1, description="Publication name")
    aliases: list[str] = Field(default_factory=list, description="Former names, short names")

    # Digital presence
    domains: list[str] = Field(
        default_factory=list,
        description="Associated domains (e.g., ['example.com', 'news.example.com'])",
    )
    platform_accounts: list[UUID] = Field(
        default_factory=list, description="References to PlatformAccount entities"
    )

    # Classification
    media_type: MediaType = Field(
        default=MediaType.DIGITAL, description="Primary media format"
    )
    editorial_focus: list[str] = Field(
        default_factory=list, description="Topic tags"
    )

    # Ownership (via relationships)
    owner_org_id: UUID | None = Field(
        default=None, description="Reference to Organization if known"
    )

    @field_validator("domains")
    @classmethod
    def validate_domains(cls, v: list[str]) -> list[str]:
        """Validate domain format (no protocol)."""
        import re

        domain_pattern = re.compile(
            r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+"
            r"[a-zA-Z]{2,}$"
        )
        for domain in v:
            if not domain_pattern.match(domain):
                raise ValueError(f"Invalid domain format: {domain}")
        return v


# =============================================================================
# Sponsor
# =============================================================================


class Sponsor(EntityBase):
    """Entities providing financial support.

    Sponsors are typically created when a funding source is identified
    before full organization resolution (e.g., from ad disclaimers).
    """

    entity_type: EntityType = Field(default=EntityType.SPONSOR, frozen=True)

    # Identifying attributes
    name: str = Field(..., min_length=1, description="Funding entity name")
    resolved_org_id: UUID | None = Field(
        default=None, description="Link to Organization after resolution"
    )

    # Meta Ad Library specific
    meta_page_id: str | None = Field(default=None, description="Meta/Facebook Page ID")
    meta_disclaimer_text: str | None = Field(
        default=None, description="Original disclaimer text"
    )

    @property
    def is_resolved(self) -> bool:
        """Check if this sponsor has been resolved to an organization."""
        return self.resolved_org_id is not None


# =============================================================================
# Create/Update Schemas
# =============================================================================


class OrganizationCreate(BaseModel):
    """Schema for creating a new organization."""

    name: str = Field(..., min_length=2)
    aliases: list[str] = Field(default_factory=list)
    org_type: OrgType = OrgType.UNKNOWN
    jurisdiction: str = "US"
    registration_id: str | None = None
    ein: str | None = None
    bn: str | None = None
    opencorp_id: str | None = None
    incorporation_date: date | None = None
    status: OrgStatus = OrgStatus.UNKNOWN
    address: Address | None = None

    model_config = ConfigDict(use_enum_values=True)


class OrganizationUpdate(BaseModel):
    """Schema for updating an organization."""

    name: str | None = None
    aliases: list[str] | None = None
    org_type: OrgType | None = None
    jurisdiction: str | None = None
    registration_id: str | None = None
    ein: str | None = None
    bn: str | None = None
    opencorp_id: str | None = None
    incorporation_date: date | None = None
    status: OrgStatus | None = None
    address: Address | None = None

    model_config = ConfigDict(use_enum_values=True)


class PersonCreate(BaseModel):
    """Schema for creating a new person."""

    name: str = Field(..., min_length=2)
    aliases: list[str] = Field(default_factory=list)
    irs_990_name: str | None = None
    opencorp_officer_id: str | None = None
    location: str | None = None

    model_config = ConfigDict(use_enum_values=True)


class PersonUpdate(BaseModel):
    """Schema for updating a person."""

    name: str | None = None
    aliases: list[str] | None = None
    irs_990_name: str | None = None
    opencorp_officer_id: str | None = None
    location: str | None = None

    model_config = ConfigDict(use_enum_values=True)


class OutletCreate(BaseModel):
    """Schema for creating a new outlet."""

    name: str = Field(..., min_length=1)
    aliases: list[str] = Field(default_factory=list)
    domains: list[str] = Field(default_factory=list)
    platform_accounts: list[UUID] = Field(default_factory=list)
    media_type: MediaType = MediaType.DIGITAL
    editorial_focus: list[str] = Field(default_factory=list)
    owner_org_id: UUID | None = None

    model_config = ConfigDict(use_enum_values=True)


class OutletUpdate(BaseModel):
    """Schema for updating an outlet."""

    name: str | None = None
    aliases: list[str] | None = None
    domains: list[str] | None = None
    platform_accounts: list[UUID] | None = None
    media_type: MediaType | None = None
    editorial_focus: list[str] | None = None
    owner_org_id: UUID | None = None

    model_config = ConfigDict(use_enum_values=True)


class SponsorCreate(BaseModel):
    """Schema for creating a new sponsor."""

    name: str = Field(..., min_length=1)
    resolved_org_id: UUID | None = None
    meta_page_id: str | None = None
    meta_disclaimer_text: str | None = None

    model_config = ConfigDict(use_enum_values=True)


class SponsorUpdate(BaseModel):
    """Schema for updating a sponsor."""

    name: str | None = None
    resolved_org_id: UUID | None = None
    meta_page_id: str | None = None
    meta_disclaimer_text: str | None = None

    model_config = ConfigDict(use_enum_values=True)


# =============================================================================
# Domain (Infrastructure Tracking)
# =============================================================================


class Domain(EntityBase):
    """Web domain with infrastructure metadata for detecting shared resources.

    Domains track hosting, DNS, analytics, and other technical signals
    that can reveal hidden organizational connections.
    """

    entity_type: EntityType = Field(default=EntityType.DOMAIN, frozen=True)

    # Identifying attributes
    name: str = Field(..., min_length=3, description="Domain name (e.g., example.com)")
    aliases: list[str] = Field(
        default_factory=list, description="Subdomains, related domains"
    )

    # Linked outlet
    outlet_id: UUID | None = Field(
        default=None, description="Reference to associated Outlet"
    )

    # DNS/Hosting infrastructure
    registrar: str | None = Field(default=None, description="Domain registrar name")
    registration_date: date | None = Field(default=None, description="WHOIS registration date")
    expiry_date: date | None = Field(default=None, description="WHOIS expiry date")
    nameservers: list[str] = Field(
        default_factory=list, description="DNS nameserver hostnames"
    )

    # IP/Hosting
    ip_addresses: list[str] = Field(
        default_factory=list, description="Resolved A record IPs"
    )
    asn: str | None = Field(default=None, description="Autonomous System Number")
    asn_name: str | None = Field(default=None, description="ASN organization name")
    hosting_provider: str | None = Field(default=None, description="Detected hosting provider")
    cdn_provider: str | None = Field(default=None, description="CDN provider (Cloudflare, Fastly, etc.)")

    # Analytics and tracking
    google_analytics_ids: list[str] = Field(
        default_factory=list, description="GA tracking IDs (UA-XXXXX, G-XXXXX)"
    )
    google_tag_manager_ids: list[str] = Field(
        default_factory=list, description="GTM container IDs"
    )
    facebook_pixel_ids: list[str] = Field(
        default_factory=list, description="Facebook/Meta pixel IDs"
    )
    adsense_ids: list[str] = Field(
        default_factory=list, description="Google AdSense publisher IDs"
    )

    # SSL/TLS
    ssl_issuer: str | None = Field(default=None, description="SSL certificate issuer")
    ssl_subject_alt_names: list[str] = Field(
        default_factory=list, description="SAN entries from SSL cert"
    )

    # Tech stack detection
    cms_platform: str | None = Field(
        default=None, description="CMS (WordPress, Drupal, etc.)"
    )
    detected_technologies: list[str] = Field(
        default_factory=list, description="Detected tech stack via fingerprinting"
    )

    # Metadata
    last_scanned: datetime | None = Field(
        default=None, description="Last infrastructure scan timestamp"
    )

    @field_validator("name")
    @classmethod
    def validate_domain_name(cls, v: str) -> str:
        """Validate domain format (no protocol)."""
        import re

        domain_pattern = re.compile(
            r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+"
            r"[a-zA-Z]{2,}$"
        )
        if not domain_pattern.match(v):
            raise ValueError(f"Invalid domain format: {v}")
        return v.lower()

    @field_validator("ip_addresses")
    @classmethod
    def validate_ip_addresses(cls, v: list[str]) -> list[str]:
        """Validate IP address format."""
        import re

        ipv4_pattern = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
        ipv6_pattern = re.compile(r"^[0-9a-fA-F:]+$")
        for ip in v:
            if not (ipv4_pattern.match(ip) or ipv6_pattern.match(ip)):
                raise ValueError(f"Invalid IP address format: {ip}")
        return v


class DomainCreate(BaseModel):
    """Schema for creating a new domain."""

    name: str = Field(..., min_length=3)
    aliases: list[str] = Field(default_factory=list)
    outlet_id: UUID | None = None
    registrar: str | None = None
    registration_date: date | None = None
    expiry_date: date | None = None
    nameservers: list[str] = Field(default_factory=list)
    ip_addresses: list[str] = Field(default_factory=list)
    asn: str | None = None
    asn_name: str | None = None
    hosting_provider: str | None = None
    cdn_provider: str | None = None
    google_analytics_ids: list[str] = Field(default_factory=list)
    google_tag_manager_ids: list[str] = Field(default_factory=list)
    facebook_pixel_ids: list[str] = Field(default_factory=list)
    adsense_ids: list[str] = Field(default_factory=list)
    ssl_issuer: str | None = None
    ssl_subject_alt_names: list[str] = Field(default_factory=list)
    cms_platform: str | None = None
    detected_technologies: list[str] = Field(default_factory=list)

    model_config = ConfigDict(use_enum_values=True)


class DomainUpdate(BaseModel):
    """Schema for updating a domain."""

    name: str | None = None
    aliases: list[str] | None = None
    outlet_id: UUID | None = None
    registrar: str | None = None
    registration_date: date | None = None
    expiry_date: date | None = None
    nameservers: list[str] | None = None
    ip_addresses: list[str] | None = None
    asn: str | None = None
    asn_name: str | None = None
    hosting_provider: str | None = None
    cdn_provider: str | None = None
    google_analytics_ids: list[str] | None = None
    google_tag_manager_ids: list[str] | None = None
    facebook_pixel_ids: list[str] | None = None
    adsense_ids: list[str] | None = None
    ssl_issuer: str | None = None
    ssl_subject_alt_names: list[str] | None = None
    cms_platform: str | None = None
    detected_technologies: list[str] | None = None
    last_scanned: datetime | None = None

    model_config = ConfigDict(use_enum_values=True)


# =============================================================================
# Vendor (Infrastructure Providers)
# =============================================================================


class VendorType(str, Enum):
    """Types of infrastructure vendors."""

    HOSTING = "hosting"
    CDN = "cdn"
    DNS = "dns"
    REGISTRAR = "registrar"
    ANALYTICS = "analytics"
    ADVERTISING = "advertising"
    CMS = "cms"
    EMAIL = "email"
    PAYMENT = "payment"
    OTHER = "other"


class Vendor(EntityBase):
    """Infrastructure service providers (hosting, analytics, CDN, etc.).

    Vendors represent technical service providers that multiple outlets
    might use, helping identify infrastructure sharing patterns.
    """

    entity_type: EntityType = Field(default=EntityType.VENDOR, frozen=True)

    # Identifying attributes
    name: str = Field(..., min_length=1, description="Vendor name")
    aliases: list[str] = Field(
        default_factory=list, description="Alternative names, acquired brands"
    )

    # Classification
    vendor_type: VendorType = Field(
        default=VendorType.OTHER, description="Primary vendor category"
    )
    secondary_types: list[VendorType] = Field(
        default_factory=list, description="Additional service categories"
    )

    # Identifiers
    website: str | None = Field(default=None, description="Vendor website URL")
    asn_numbers: list[str] = Field(
        default_factory=list, description="ASN numbers owned by vendor"
    )
    ip_ranges: list[str] = Field(
        default_factory=list, description="CIDR ranges owned by vendor"
    )
    nameserver_patterns: list[str] = Field(
        default_factory=list, description="Regex patterns for vendor nameservers"
    )

    # Metadata
    is_major_provider: bool = Field(
        default=False, description="Major cloud/CDN provider (reduces signal value)"
    )


class VendorCreate(BaseModel):
    """Schema for creating a new vendor."""

    name: str = Field(..., min_length=1)
    aliases: list[str] = Field(default_factory=list)
    vendor_type: VendorType = VendorType.OTHER
    secondary_types: list[VendorType] = Field(default_factory=list)
    website: str | None = None
    asn_numbers: list[str] = Field(default_factory=list)
    ip_ranges: list[str] = Field(default_factory=list)
    nameserver_patterns: list[str] = Field(default_factory=list)
    is_major_provider: bool = False

    model_config = ConfigDict(use_enum_values=True)


class VendorUpdate(BaseModel):
    """Schema for updating a vendor."""

    name: str | None = None
    aliases: list[str] | None = None
    vendor_type: VendorType | None = None
    secondary_types: list[VendorType] | None = None
    website: str | None = None
    asn_numbers: list[str] | None = None
    ip_ranges: list[str] | None = None
    nameserver_patterns: list[str] | None = None
    is_major_provider: bool | None = None

    model_config = ConfigDict(use_enum_values=True)


# =============================================================================
# PlatformAccount (Social Media Accounts)
# =============================================================================


class PlatformType(str, Enum):
    """Types of social media platforms."""

    FACEBOOK = "facebook"
    TWITTER = "twitter"
    INSTAGRAM = "instagram"
    YOUTUBE = "youtube"
    TIKTOK = "tiktok"
    LINKEDIN = "linkedin"
    THREADS = "threads"
    BLUESKY = "bluesky"
    MASTODON = "mastodon"
    TELEGRAM = "telegram"
    REDDIT = "reddit"
    OTHER = "other"


class PlatformAccount(EntityBase):
    """Social media accounts associated with outlets or organizations.

    Tracks social media presence and can help identify coordinated
    behavior or shared management across outlets.
    """

    entity_type: EntityType = Field(default=EntityType.PLATFORM_ACCOUNT, frozen=True)

    # Identifying attributes
    name: str = Field(..., min_length=1, description="Display name / handle")
    aliases: list[str] = Field(
        default_factory=list, description="Former handles, display names"
    )

    # Platform details
    platform: PlatformType = Field(..., description="Social media platform")
    platform_id: str | None = Field(
        default=None, description="Platform-specific unique ID"
    )
    handle: str | None = Field(default=None, description="Username / handle")
    profile_url: str | None = Field(default=None, description="Profile URL")

    # Linked entities
    outlet_id: UUID | None = Field(
        default=None, description="Reference to associated Outlet"
    )
    organization_id: UUID | None = Field(
        default=None, description="Reference to associated Organization"
    )

    # Metadata
    follower_count: int | None = Field(default=None, description="Follower count")
    following_count: int | None = Field(default=None, description="Following count")
    post_count: int | None = Field(default=None, description="Total posts/tweets")
    verified: bool = Field(default=False, description="Platform verification status")
    created_at_platform: datetime | None = Field(
        default=None, description="Account creation date on platform"
    )

    # Business account info
    is_business_account: bool = Field(default=False, description="Business/creator account")
    linked_page_id: str | None = Field(
        default=None, description="Linked Facebook Page ID for Instagram"
    )


class PlatformAccountCreate(BaseModel):
    """Schema for creating a new platform account."""

    name: str = Field(..., min_length=1)
    aliases: list[str] = Field(default_factory=list)
    platform: PlatformType
    platform_id: str | None = None
    handle: str | None = None
    profile_url: str | None = None
    outlet_id: UUID | None = None
    organization_id: UUID | None = None
    follower_count: int | None = None
    following_count: int | None = None
    post_count: int | None = None
    verified: bool = False
    created_at_platform: datetime | None = None
    is_business_account: bool = False
    linked_page_id: str | None = None

    model_config = ConfigDict(use_enum_values=True)


class PlatformAccountUpdate(BaseModel):
    """Schema for updating a platform account."""

    name: str | None = None
    aliases: list[str] | None = None
    platform: PlatformType | None = None
    platform_id: str | None = None
    handle: str | None = None
    profile_url: str | None = None
    outlet_id: UUID | None = None
    organization_id: UUID | None = None
    follower_count: int | None = None
    following_count: int | None = None
    post_count: int | None = None
    verified: bool | None = None
    created_at_platform: datetime | None = None
    is_business_account: bool | None = None
    linked_page_id: str | None = None

    model_config = ConfigDict(use_enum_values=True)
