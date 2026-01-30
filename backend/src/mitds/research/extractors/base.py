"""Base class for lead extractors.

Lead extractors discover new leads from entities by querying
various data sources (databases, Neo4j, external APIs).
"""

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator
from uuid import UUID

from ...logging import get_context_logger
from ..models import (
    IdentifierType,
    Lead,
    LeadType,
    ResearchSessionConfig,
)

logger = get_context_logger(__name__)


class BaseLeadExtractor(ABC):
    """Abstract base class for lead extraction.

    Lead extractors discover new leads from existing entities
    by querying databases, Neo4j, or external data sources.

    Subclasses must implement:
    - supported_entity_types: Which entity types this extractor handles
    - lead_sources: Which data sources are queried
    - extract_leads(): The actual lead extraction logic
    """

    @property
    @abstractmethod
    def supported_entity_types(self) -> list[str]:
        """Entity types this extractor works with.

        Returns:
            List of entity type names (e.g., ["ORGANIZATION", "PERSON"])
        """
        ...

    @property
    @abstractmethod
    def lead_sources(self) -> list[str]:
        """Data sources this extractor queries.

        Returns:
            List of source names (e.g., ["sec_edgar", "sedar"])
        """
        ...

    @property
    def name(self) -> str:
        """Extractor name for logging."""
        return self.__class__.__name__

    def supports_entity_type(self, entity_type: str) -> bool:
        """Check if this extractor supports an entity type.

        Args:
            entity_type: Entity type to check

        Returns:
            True if supported
        """
        return entity_type.upper() in [t.upper() for t in self.supported_entity_types]

    @abstractmethod
    async def extract_leads(
        self,
        entity_id: UUID,
        entity_type: str,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract leads from an entity.

        Args:
            entity_id: Entity UUID
            entity_type: Entity type
            entity_name: Entity name
            entity_data: Entity metadata/properties
            config: Session configuration for filtering

        Yields:
            Lead objects discovered from this entity
        """
        ...

    async def extract_leads_batch(
        self,
        entities: list[dict[str, Any]],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[tuple[UUID, Lead]]:
        """Extract leads from multiple entities.

        Args:
            entities: List of entity dicts with id, type, name, data
            config: Session configuration

        Yields:
            Tuples of (source_entity_id, Lead)
        """
        for entity in entities:
            entity_id = entity["id"]
            entity_type = entity["type"]
            entity_name = entity["name"]
            entity_data = entity.get("data", {})

            if not self.supports_entity_type(entity_type):
                continue

            try:
                async for lead in self.extract_leads(
                    entity_id, entity_type, entity_name, entity_data, config
                ):
                    yield (entity_id, lead)
            except Exception as e:
                logger.warning(
                    f"{self.name}: Failed to extract leads from {entity_name}: {e}"
                )
                continue

    def create_lead(
        self,
        lead_type: LeadType,
        target_identifier: str,
        identifier_type: IdentifierType,
        priority: int = 3,
        confidence: float = 0.8,
        context: dict[str, Any] | None = None,
        source_relationship_type: str | None = None,
    ) -> Lead:
        """Helper to create a Lead object.

        Args:
            lead_type: Type of lead
            target_identifier: Name, ID, or query
            identifier_type: Type of identifier
            priority: Priority (1=highest, 5=lowest)
            confidence: Confidence score
            context: Additional context
            source_relationship_type: Relationship type that generated this lead

        Returns:
            New Lead object
        """
        return Lead(
            lead_type=lead_type,
            target_identifier=target_identifier,
            target_identifier_type=identifier_type,
            priority=priority,
            confidence=confidence,
            context=context or {},
            source_relationship_type=source_relationship_type,
        )

    def should_follow_lead(
        self,
        lead: Lead,
        config: ResearchSessionConfig,
    ) -> bool:
        """Check if a lead should be followed based on config.

        Args:
            lead: Lead to check
            config: Session configuration

        Returns:
            True if lead should be followed
        """
        # Check lead type is enabled
        if config.enabled_lead_types and lead.lead_type not in config.enabled_lead_types:
            return False

        # Check confidence threshold
        if lead.confidence < config.min_confidence:
            return False

        # Check funding amount if applicable
        if config.min_funding_amount is not None:
            amount = lead.context.get("amount")
            if amount is not None and amount < config.min_funding_amount:
                return False

        return True

    def apply_priority_boost(
        self,
        base_priority: int,
        lead_type: LeadType,
        config: ResearchSessionConfig,
    ) -> int:
        """Apply priority boost from config.

        Args:
            base_priority: Base priority (1-5)
            lead_type: Lead type
            config: Session configuration

        Returns:
            Adjusted priority (clamped to 1-5)
        """
        boost = 0
        if lead_type == LeadType.OWNERSHIP:
            boost = config.ownership_priority_boost
        elif lead_type == LeadType.FUNDING:
            boost = config.funding_priority_boost

        adjusted = base_priority - boost  # Lower number = higher priority
        return max(1, min(5, adjusted))
