"""Synthetic coordination pattern generator for MITDS validation.

Generates synthetic test cases with known patterns for algorithm validation.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID, uuid4


class PatternType(str, Enum):
    """Types of synthetic patterns that can be generated."""

    TEMPORAL_BURST = "temporal_burst"  # Coordinated timing
    FUNDING_CLUSTER = "funding_cluster"  # Shared funders
    INFRASTRUCTURE_SHARED = "infrastructure_shared"  # Shared tech
    BOARD_OVERLAP = "board_overlap"  # Shared board members
    CONTENT_SIMILARITY = "content_similarity"  # Similar content
    ORGANIC_NOISE = "organic_noise"  # Random noise (negative)
    BREAKING_NEWS = "breaking_news"  # Legitimate clustering


@dataclass
class SyntheticEntity:
    """A synthetic entity for testing."""

    id: UUID = field(default_factory=uuid4)
    name: str = ""
    entity_type: str = "organization"
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class SyntheticEvent:
    """A synthetic event (publication, social post, etc.)."""

    id: UUID = field(default_factory=uuid4)
    entity_id: UUID = field(default_factory=uuid4)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    event_type: str = "publication"
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class SyntheticRelationship:
    """A synthetic relationship between entities."""

    id: UUID = field(default_factory=uuid4)
    source_id: UUID = field(default_factory=uuid4)
    target_id: UUID = field(default_factory=uuid4)
    relationship_type: str = "FUNDED_BY"
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class CoordinationPattern:
    """A synthetic coordination pattern for validation."""

    id: UUID = field(default_factory=uuid4)
    pattern_type: PatternType = PatternType.ORGANIC_NOISE
    label: str = "negative"  # "positive" or "negative"
    description: str = ""
    entities: list[SyntheticEntity] = field(default_factory=list)
    events: list[SyntheticEvent] = field(default_factory=list)
    relationships: list[SyntheticRelationship] = field(default_factory=list)
    expected_score_range: tuple[float, float] = (0.0, 1.0)
    expected_signals: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": str(self.id),
            "pattern_type": self.pattern_type.value,
            "label": self.label,
            "description": self.description,
            "entities": [
                {
                    "id": str(e.id),
                    "name": e.name,
                    "entity_type": e.entity_type,
                    "properties": e.properties,
                }
                for e in self.entities
            ],
            "events": [
                {
                    "id": str(e.id),
                    "entity_id": str(e.entity_id),
                    "timestamp": e.timestamp.isoformat(),
                    "event_type": e.event_type,
                    "properties": e.properties,
                }
                for e in self.events
            ],
            "relationships": [
                {
                    "id": str(r.id),
                    "source_id": str(r.source_id),
                    "target_id": str(r.target_id),
                    "relationship_type": r.relationship_type,
                    "properties": r.properties,
                }
                for r in self.relationships
            ],
            "expected_score_range": self.expected_score_range,
            "expected_signals": self.expected_signals,
            "metadata": self.metadata,
        }


class SyntheticGenerator:
    """Generator for synthetic coordination patterns."""

    def __init__(self, seed: int | None = None):
        """Initialize generator with optional random seed."""
        self.rng = random.Random(seed)

    def generate_temporal_burst(
        self,
        entity_count: int = 5,
        event_count_per_entity: int = 3,
        window_minutes: int = 30,
        base_time: datetime | None = None,
    ) -> CoordinationPattern:
        """Generate a temporal coordination burst pattern.

        Creates multiple entities publishing within a narrow time window,
        which should trigger temporal coordination detection.
        """
        base_time = base_time or datetime.utcnow()

        entities = [
            SyntheticEntity(
                name=f"Outlet_{i}",
                entity_type="outlet",
                properties={"synthetic": True},
            )
            for i in range(entity_count)
        ]

        events = []
        for entity in entities:
            for j in range(event_count_per_entity):
                # Cluster events within the window
                offset_minutes = self.rng.uniform(0, window_minutes)
                timestamp = base_time + timedelta(minutes=offset_minutes)
                events.append(
                    SyntheticEvent(
                        entity_id=entity.id,
                        timestamp=timestamp,
                        event_type="publication",
                        properties={
                            "title": f"Article about Topic X - {entity.name}",
                            "synthetic": True,
                        },
                    )
                )

        return CoordinationPattern(
            pattern_type=PatternType.TEMPORAL_BURST,
            label="positive",
            description=(
                f"Synthetic temporal burst: {entity_count} entities publishing "
                f"{event_count_per_entity} events each within {window_minutes} minutes"
            ),
            entities=entities,
            events=events,
            expected_score_range=(0.5, 0.9),
            expected_signals=["temporal_coordination", "burst_detection"],
            metadata={
                "window_minutes": window_minutes,
                "entity_count": entity_count,
            },
        )

    def generate_funding_cluster(
        self,
        outlet_count: int = 4,
        funder_count: int = 2,
        funding_concentration: float = 0.8,
    ) -> CoordinationPattern:
        """Generate a funding cluster pattern.

        Creates outlets with concentrated funding from few sources,
        which should trigger funding concentration detection.
        """
        funders = [
            SyntheticEntity(
                name=f"Foundation_{i}",
                entity_type="organization",
                properties={"org_type": "foundation", "synthetic": True},
            )
            for i in range(funder_count)
        ]

        outlets = [
            SyntheticEntity(
                name=f"Outlet_{i}",
                entity_type="outlet",
                properties={"synthetic": True},
            )
            for i in range(outlet_count)
        ]

        relationships = []
        for outlet in outlets:
            # Each outlet funded primarily by the concentrated funders
            total_funding = self.rng.uniform(100000, 1000000)
            concentrated_funding = total_funding * funding_concentration
            per_funder = concentrated_funding / funder_count

            for funder in funders:
                relationships.append(
                    SyntheticRelationship(
                        source_id=outlet.id,
                        target_id=funder.id,
                        relationship_type="FUNDED_BY",
                        properties={
                            "amount": per_funder,
                            "year": 2024,
                            "synthetic": True,
                        },
                    )
                )

        return CoordinationPattern(
            pattern_type=PatternType.FUNDING_CLUSTER,
            label="positive",
            description=(
                f"Synthetic funding cluster: {outlet_count} outlets receiving "
                f"{funding_concentration:.0%} of funding from {funder_count} funders"
            ),
            entities=funders + outlets,
            relationships=relationships,
            expected_score_range=(0.45, 0.85),
            expected_signals=["funding_concentration", "shared_funder"],
            metadata={
                "funding_concentration": funding_concentration,
                "funder_count": funder_count,
            },
        )

    def generate_shared_infrastructure(
        self,
        domain_count: int = 5,
        shared_analytics: bool = True,
        shared_hosting: bool = True,
    ) -> CoordinationPattern:
        """Generate a shared infrastructure pattern.

        Creates domains sharing technical infrastructure markers,
        which should trigger infrastructure sharing detection.
        """
        # Shared infrastructure values
        analytics_id = f"UA-{self.rng.randint(100000, 999999)}-1"
        hosting_provider = "SharedHostCo"
        ip_address = f"192.168.{self.rng.randint(1, 254)}.{self.rng.randint(1, 254)}"

        domains = []
        for i in range(domain_count):
            props = {"synthetic": True}
            if shared_analytics:
                props["google_analytics_id"] = analytics_id
            if shared_hosting:
                props["hosting_provider"] = hosting_provider
                props["ip_address"] = ip_address

            domains.append(
                SyntheticEntity(
                    name=f"domain{i}.example.com",
                    entity_type="domain",
                    properties=props,
                )
            )

        expected_signals = []
        if shared_analytics:
            expected_signals.append("same_analytics")
        if shared_hosting:
            expected_signals.extend(["same_hosting", "same_ip"])

        return CoordinationPattern(
            pattern_type=PatternType.INFRASTRUCTURE_SHARED,
            label="positive",
            description=(
                f"Synthetic shared infrastructure: {domain_count} domains sharing "
                f"analytics={shared_analytics}, hosting={shared_hosting}"
            ),
            entities=domains,
            expected_score_range=(0.5, 0.95),
            expected_signals=expected_signals,
            metadata={
                "shared_analytics": shared_analytics,
                "shared_hosting": shared_hosting,
                "analytics_id": analytics_id if shared_analytics else None,
            },
        )

    def generate_board_overlap(
        self,
        org_count: int = 4,
        shared_board_member_count: int = 2,
    ) -> CoordinationPattern:
        """Generate a board overlap pattern.

        Creates organizations sharing board members,
        which should contribute to coordination scoring.
        """
        shared_members = [
            SyntheticEntity(
                name=f"Person_{i}",
                entity_type="person",
                properties={"synthetic": True},
            )
            for i in range(shared_board_member_count)
        ]

        organizations = [
            SyntheticEntity(
                name=f"Organization_{i}",
                entity_type="organization",
                properties={"synthetic": True},
            )
            for i in range(org_count)
        ]

        relationships = []
        for org in organizations:
            for member in shared_members:
                relationships.append(
                    SyntheticRelationship(
                        source_id=member.id,
                        target_id=org.id,
                        relationship_type="DIRECTOR_OF",
                        properties={"synthetic": True},
                    )
                )

        return CoordinationPattern(
            pattern_type=PatternType.BOARD_OVERLAP,
            label="positive",
            description=(
                f"Synthetic board overlap: {shared_board_member_count} people serving "
                f"on boards of all {org_count} organizations"
            ),
            entities=shared_members + organizations,
            relationships=relationships,
            expected_score_range=(0.3, 0.7),
            expected_signals=["board_overlap", "governance_concentration"],
            metadata={
                "overlap_count": shared_board_member_count,
                "org_count": org_count,
            },
        )

    def generate_organic_noise(
        self,
        entity_count: int = 10,
        event_count: int = 50,
        hours_span: int = 168,  # 1 week
    ) -> CoordinationPattern:
        """Generate organic noise (hard negative).

        Creates random, non-coordinated activity that should
        NOT trigger coordination detection.
        """
        base_time = datetime.utcnow() - timedelta(hours=hours_span)

        entities = [
            SyntheticEntity(
                name=f"RandomOutlet_{i}",
                entity_type="outlet",
                properties={"synthetic": True, "independent": True},
            )
            for i in range(entity_count)
        ]

        # Distribute events randomly across time and entities
        events = []
        for _ in range(event_count):
            entity = self.rng.choice(entities)
            offset_hours = self.rng.uniform(0, hours_span)
            timestamp = base_time + timedelta(hours=offset_hours)
            events.append(
                SyntheticEvent(
                    entity_id=entity.id,
                    timestamp=timestamp,
                    event_type="publication",
                    properties={"synthetic": True, "topic": f"Topic_{self.rng.randint(1, 100)}"},
                )
            )

        return CoordinationPattern(
            pattern_type=PatternType.ORGANIC_NOISE,
            label="negative",
            description=(
                f"Synthetic organic noise: {event_count} random events from "
                f"{entity_count} independent entities over {hours_span} hours"
            ),
            entities=entities,
            events=events,
            expected_score_range=(0.0, 0.3),
            expected_signals=[],  # Should NOT detect coordination
            metadata={
                "hours_span": hours_span,
                "events_per_entity": event_count / entity_count,
            },
        )

    def generate_breaking_news(
        self,
        outlet_count: int = 20,
        events_per_outlet: int = 2,
        window_minutes: int = 60,
    ) -> CoordinationPattern:
        """Generate breaking news pattern (hard negative).

        Creates multiple outlets covering the same breaking news event.
        Should NOT trigger coordination detection due to hard negative filters.
        """
        base_time = datetime.utcnow()

        entities = [
            SyntheticEntity(
                name=f"NewsOutlet_{i}",
                entity_type="outlet",
                properties={"synthetic": True, "outlet_type": "news"},
            )
            for i in range(outlet_count)
        ]

        # All outlets covering the same breaking news topic
        news_topic = "Major Breaking News Event"
        events = []
        for entity in entities:
            for j in range(events_per_outlet):
                offset_minutes = self.rng.uniform(0, window_minutes)
                timestamp = base_time + timedelta(minutes=offset_minutes)
                events.append(
                    SyntheticEvent(
                        entity_id=entity.id,
                        timestamp=timestamp,
                        event_type="publication",
                        properties={
                            "topic": news_topic,
                            "is_breaking_news": True,
                            "synthetic": True,
                        },
                    )
                )

        return CoordinationPattern(
            pattern_type=PatternType.BREAKING_NEWS,
            label="negative",
            description=(
                f"Synthetic breaking news: {outlet_count} outlets covering "
                f"same news event within {window_minutes} minutes (legitimate)"
            ),
            entities=entities,
            events=events,
            expected_score_range=(0.0, 0.25),
            expected_signals=[],  # Hard negative filter should suppress
            metadata={
                "window_minutes": window_minutes,
                "is_breaking_news": True,
            },
        )


def generate_synthetic_case(
    pattern_type: PatternType,
    seed: int | None = None,
    **kwargs: Any,
) -> CoordinationPattern:
    """Generate a synthetic test case of the specified type.

    Args:
        pattern_type: Type of pattern to generate
        seed: Random seed for reproducibility
        **kwargs: Additional arguments passed to generator method

    Returns:
        Generated CoordinationPattern
    """
    generator = SyntheticGenerator(seed=seed)

    generators = {
        PatternType.TEMPORAL_BURST: generator.generate_temporal_burst,
        PatternType.FUNDING_CLUSTER: generator.generate_funding_cluster,
        PatternType.INFRASTRUCTURE_SHARED: generator.generate_shared_infrastructure,
        PatternType.BOARD_OVERLAP: generator.generate_board_overlap,
        PatternType.ORGANIC_NOISE: generator.generate_organic_noise,
        PatternType.BREAKING_NEWS: generator.generate_breaking_news,
    }

    if pattern_type not in generators:
        raise ValueError(f"Unknown pattern type: {pattern_type}")

    return generators[pattern_type](**kwargs)


def generate_validation_suite(
    seed: int | None = None,
    positive_per_type: int = 3,
    negative_count: int = 10,
) -> list[CoordinationPattern]:
    """Generate a complete validation suite with positive and negative cases.

    Args:
        seed: Random seed for reproducibility
        positive_per_type: Number of positive cases per pattern type
        negative_count: Number of hard negative cases

    Returns:
        List of CoordinationPattern instances
    """
    generator = SyntheticGenerator(seed=seed)
    patterns = []

    # Generate positive cases
    positive_types = [
        PatternType.TEMPORAL_BURST,
        PatternType.FUNDING_CLUSTER,
        PatternType.INFRASTRUCTURE_SHARED,
        PatternType.BOARD_OVERLAP,
    ]

    for pattern_type in positive_types:
        for i in range(positive_per_type):
            pattern = generate_synthetic_case(
                pattern_type,
                seed=seed + i if seed else None,
            )
            patterns.append(pattern)

    # Generate hard negatives
    negative_types = [PatternType.ORGANIC_NOISE, PatternType.BREAKING_NEWS]
    per_type = negative_count // len(negative_types)

    for pattern_type in negative_types:
        for i in range(per_type):
            pattern = generate_synthetic_case(
                pattern_type,
                seed=seed + 100 + i if seed else None,
            )
            patterns.append(pattern)

    return patterns
