"""Political funding obfuscation detection for MITDS.

Detects patterns where corporations may be obfuscating their funding
of third-party advertisers (TPAs) through various indirect relationships.

Key signals:
1. Direct election contributions
2. Shared beneficial ownership
3. Lobbying client relationships
4. Shared directors
5. Shared addresses
6. PPSA secured interests
7. Shared incorporation agents
8. Shell company heuristics
9. Temporal correlation
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger

logger = get_context_logger(__name__)


class PoliticalFundingSignalType(str, Enum):
    """Types of political funding obfuscation signals."""

    ELECTION_CONTRIBUTION = "election_contribution"
    SHARED_BENEFICIAL_OWNER = "shared_beneficial_owner"
    LOBBYING_CLIENT = "lobbying_client"
    SHARED_DIRECTORS = "shared_directors"
    SHARED_ADDRESS = "shared_address"
    PPSA_SECURED_INTEREST = "ppsa_secured_interest"
    SHARED_AGENT = "shared_agent"
    SHELL_HEURISTIC = "shell_heuristic"
    TEMPORAL_CORRELATION = "temporal_correlation"


class PoliticalFundingCategory(str, Enum):
    """Categories for grouping related signals."""

    FUNDING = "funding"
    CONTROL = "control"
    SERVICE = "service"
    INFRASTRUCTURE = "infrastructure"
    FINANCIAL = "financial"
    BEHAVIORAL = "behavioral"
    TEMPORAL = "temporal"


# Mapping of signals to categories
SIGNAL_CATEGORIES = {
    PoliticalFundingSignalType.ELECTION_CONTRIBUTION: PoliticalFundingCategory.FUNDING,
    PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER: PoliticalFundingCategory.CONTROL,
    PoliticalFundingSignalType.LOBBYING_CLIENT: PoliticalFundingCategory.SERVICE,
    PoliticalFundingSignalType.SHARED_DIRECTORS: PoliticalFundingCategory.CONTROL,
    PoliticalFundingSignalType.SHARED_ADDRESS: PoliticalFundingCategory.INFRASTRUCTURE,
    PoliticalFundingSignalType.PPSA_SECURED_INTEREST: PoliticalFundingCategory.FINANCIAL,
    PoliticalFundingSignalType.SHARED_AGENT: PoliticalFundingCategory.INFRASTRUCTURE,
    PoliticalFundingSignalType.SHELL_HEURISTIC: PoliticalFundingCategory.BEHAVIORAL,
    PoliticalFundingSignalType.TEMPORAL_CORRELATION: PoliticalFundingCategory.TEMPORAL,
}

# Signal weights (base contribution to score)
SIGNAL_WEIGHTS = {
    PoliticalFundingSignalType.ELECTION_CONTRIBUTION: 0.40,
    PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER: 0.35,
    PoliticalFundingSignalType.LOBBYING_CLIENT: 0.30,
    PoliticalFundingSignalType.SHARED_DIRECTORS: 0.25,
    PoliticalFundingSignalType.SHARED_ADDRESS: 0.15,
    PoliticalFundingSignalType.PPSA_SECURED_INTEREST: 0.20,
    PoliticalFundingSignalType.SHARED_AGENT: 0.10,
    PoliticalFundingSignalType.SHELL_HEURISTIC: 0.15,
    PoliticalFundingSignalType.TEMPORAL_CORRELATION: 0.10,
}

# Minimum requirements for flagging
MINIMUM_SIGNALS = 2
MINIMUM_CATEGORIES = 2


@dataclass
class PoliticalFundingSignal:
    """A single detected political funding obfuscation signal."""

    signal_type: PoliticalFundingSignalType
    strength: float  # 0-1 signal strength
    confidence: float  # 0-1 confidence in detection
    entity_ids: list[UUID]
    evidence_ids: list[UUID] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def category(self) -> PoliticalFundingCategory:
        return SIGNAL_CATEGORIES.get(
            self.signal_type, PoliticalFundingCategory.FUNDING
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_type": self.signal_type.value,
            "category": self.category.value,
            "strength": self.strength,
            "confidence": self.confidence,
            "entity_ids": [str(eid) for eid in self.entity_ids],
            "evidence_ids": [str(eid) for eid in self.evidence_ids],
            "metadata": self.metadata,
            "detected_at": self.detected_at.isoformat(),
        }


class PoliticalFundingObfuscationDetector:
    """Detector for political funding obfuscation patterns.

    Identifies potential obfuscation where corporations fund third-party
    advertisers through indirect relationships rather than direct contributions.
    """

    def __init__(
        self,
        min_signals: int = MINIMUM_SIGNALS,
        min_categories: int = MINIMUM_CATEGORIES,
    ):
        """Initialize the detector.

        Args:
            min_signals: Minimum number of signals required to flag
            min_categories: Minimum number of categories required to flag
        """
        self.min_signals = min_signals
        self.min_categories = min_categories

    async def analyze(
        self,
        advertiser_entity_id: UUID,
        funder_entity_ids: list[UUID] | None = None,
        include_signals: list[str] | None = None,
    ) -> dict[str, Any]:
        """Analyze potential funding obfuscation for a third-party advertiser.

        Args:
            advertiser_entity_id: UUID of the third-party advertiser entity
            funder_entity_ids: Optional list of specific funder entity IDs to check.
                              If None, checks all potential funders.
            include_signals: Optional list of signal types to include.
                            If None, checks all signal types.

        Returns:
            Dictionary with:
            - overall_score: float (0-1)
            - is_flagged: bool
            - signal_count: int
            - category_count: int
            - signals: list of signal dicts
            - suspected_funders: list of entity IDs
        """
        signals: list[PoliticalFundingSignal] = []

        # Determine which signals to check
        signal_types_to_check = list(PoliticalFundingSignalType)
        if include_signals:
            signal_types_to_check = [
                st
                for st in signal_types_to_check
                if st.value in include_signals
            ]

        # Check each signal type
        if PoliticalFundingSignalType.ELECTION_CONTRIBUTION in signal_types_to_check:
            contribution_signal = await self._check_election_contributions(
                advertiser_entity_id, funder_entity_ids
            )
            if contribution_signal:
                signals.append(contribution_signal)

        if PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER in signal_types_to_check:
            beneficial_owner_signal = await self._check_beneficial_ownership(
                advertiser_entity_id, funder_entity_ids
            )
            if beneficial_owner_signal:
                signals.append(beneficial_owner_signal)

        if PoliticalFundingSignalType.LOBBYING_CLIENT in signal_types_to_check:
            lobbying_signal = await self._check_lobbying_clients(
                advertiser_entity_id, funder_entity_ids
            )
            if lobbying_signal:
                signals.append(lobbying_signal)

        if PoliticalFundingSignalType.SHARED_DIRECTORS in signal_types_to_check:
            directors_signal = await self._check_shared_directors(
                advertiser_entity_id, funder_entity_ids
            )
            if directors_signal:
                signals.append(directors_signal)

        if PoliticalFundingSignalType.SHARED_ADDRESS in signal_types_to_check:
            address_signal = await self._check_shared_address(
                advertiser_entity_id, funder_entity_ids
            )
            if address_signal:
                signals.append(address_signal)

        if PoliticalFundingSignalType.PPSA_SECURED_INTEREST in signal_types_to_check:
            ppsa_signal = await self._check_ppsa(
                advertiser_entity_id, funder_entity_ids
            )
            if ppsa_signal:
                signals.append(ppsa_signal)

        if PoliticalFundingSignalType.SHARED_AGENT in signal_types_to_check:
            agent_signal = await self._check_shared_agent(
                advertiser_entity_id, funder_entity_ids
            )
            if agent_signal:
                signals.append(agent_signal)

        if PoliticalFundingSignalType.SHELL_HEURISTIC in signal_types_to_check:
            shell_signal = await self._check_shell_heuristic(
                advertiser_entity_id, funder_entity_ids
            )
            if shell_signal:
                signals.append(shell_signal)

        if PoliticalFundingSignalType.TEMPORAL_CORRELATION in signal_types_to_check:
            temporal_signal = await self._check_temporal_correlation(
                advertiser_entity_id, funder_entity_ids
            )
            if temporal_signal:
                signals.append(temporal_signal)

        # Calculate overall score
        overall_score = self._calculate_score(signals)

        # Check if flagged (meets minimum requirements)
        categories = set(s.category for s in signals)
        is_flagged = (
            len(signals) >= self.min_signals
            and len(categories) >= self.min_categories
            and overall_score > 0.0
        )

        # Collect suspected funders
        suspected_funders = set()
        for signal in signals:
            suspected_funders.update(signal.entity_ids)
        suspected_funders.discard(advertiser_entity_id)

        return {
            "overall_score": round(overall_score, 4),
            "is_flagged": is_flagged,
            "signal_count": len(signals),
            "category_count": len(categories),
            "signals": [s.to_dict() for s in signals],
            "suspected_funders": [str(fid) for fid in sorted(suspected_funders)],
        }

    def _calculate_score(self, signals: list[PoliticalFundingSignal]) -> float:
        """Calculate overall obfuscation score from signals."""
        if not signals:
            return 0.0

        total_score = 0.0
        for signal in signals:
            weight = SIGNAL_WEIGHTS.get(signal.signal_type, 0.0)
            contribution = weight * signal.strength * signal.confidence
            total_score += contribution

        # Normalize to 0-1 range (cap at 1.0)
        return min(1.0, total_score)

    async def _check_election_contributions(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if corporations contributed to TPA's election return."""
        try:
            async with get_neo4j_session() as session:
                if funder_ids:
                    funder_filter = "AND funder.id IN $funder_ids"
                    params = {
                        "advertiser_id": str(advertiser_id),
                        "funder_ids": [str(fid) for fid in funder_ids],
                    }
                else:
                    funder_filter = ""
                    params = {"advertiser_id": str(advertiser_id)}

                query = f"""
                MATCH (funder)-[r:CONTRIBUTED_TO]->(tpa:Organization {{id: $advertiser_id}})
                WHERE funder.entity_type = 'ORGANIZATION'
                  {funder_filter}
                RETURN funder.id as funder_id,
                       funder.name as funder_name,
                       sum(r.amount) as total_amount,
                       count(r) as contribution_count,
                       collect(DISTINCT r.election_id) as election_ids
                ORDER BY total_amount DESC
                LIMIT 10
                """

                result = await session.run(query, **params)
                records = await result.data()

                if not records:
                    return None

                # Calculate strength based on contribution amount and count
                max_amount = max(r.get("total_amount", 0) or 0 for r in records)
                total_contributions = sum(
                    r.get("contribution_count", 0) or 0 for r in records
                )

                # Normalize strength (assume max $100k = 1.0)
                strength = min(1.0, max_amount / 100000.0)
                strength = max(strength, min(0.5, total_contributions / 5.0))

                funder_entity_ids = [
                    UUID(r["funder_id"]) for r in records if r.get("funder_id")
                ]

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
                    strength=strength,
                    confidence=0.9,  # High confidence - direct evidence
                    entity_ids=funder_entity_ids,
                    metadata={
                        "total_amount": max_amount,
                        "contribution_count": total_contributions,
                        "election_ids": [
                            eid
                            for r in records
                            for eid in (r.get("election_ids") or [])
                        ],
                    },
                )

        except Exception as e:
            logger.warning(f"Error checking election contributions: {e}")
            return None

    async def _check_beneficial_ownership(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if corporations share beneficial owners with TPA."""
        try:
            async with get_neo4j_session() as session:
                if funder_ids:
                    funder_filter = "AND corp.id IN $funder_ids"
                    params = {
                        "advertiser_id": str(advertiser_id),
                        "funder_ids": [str(fid) for fid in funder_ids],
                    }
                else:
                    funder_filter = ""
                    params = {"advertiser_id": str(advertiser_id)}

                query = f"""
                MATCH (owner:Person)-[:BENEFICIAL_OWNER_OF]->(tpa:Organization {{id: $advertiser_id}})
                MATCH (owner)-[:BENEFICIAL_OWNER_OF]->(corp:Organization)
                WHERE corp.id <> $advertiser_id
                  AND corp.entity_type = 'ORGANIZATION'
                  {funder_filter}
                RETURN DISTINCT corp.id as corp_id,
                       corp.name as corp_name,
                       owner.id as owner_id,
                       owner.name as owner_name
                LIMIT 20
                """

                result = await session.run(query, **params)
                records = await result.data()

                if not records:
                    return None

                # Count unique corporations and owners
                unique_corps = set(r["corp_id"] for r in records if r.get("corp_id"))
                unique_owners = set(
                    r["owner_id"] for r in records if r.get("owner_id")
                )

                # Strength based on number of shared owners and corporations
                strength = min(1.0, len(unique_corps) / 5.0)
                strength = max(strength, min(0.7, len(unique_owners) / 3.0))

                corp_entity_ids = [UUID(cid) for cid in unique_corps]

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER,
                    strength=strength,
                    confidence=0.85,  # High confidence - official registry data
                    entity_ids=corp_entity_ids,
                    metadata={
                        "shared_owner_count": len(unique_owners),
                        "corporation_count": len(unique_corps),
                        "owners": [
                            {"id": str(r["owner_id"]), "name": r.get("owner_name")}
                            for r in records[:5]
                        ],
                    },
                )

        except Exception as e:
            logger.warning(f"Error checking beneficial ownership: {e}")
            return None

    async def _check_lobbying_clients(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if corporations are lobbying clients of TPA."""
        try:
            async with get_neo4j_session() as session:
                if funder_ids:
                    funder_filter = "AND client.id IN $funder_ids"
                    params = {
                        "advertiser_id": str(advertiser_id),
                        "funder_ids": [str(fid) for fid in funder_ids],
                    }
                else:
                    funder_filter = ""
                    params = {"advertiser_id": str(advertiser_id)}

                query = f"""
                MATCH (lobbyist)-[:LOBBIES_FOR]->(tpa:Organization {{id: $advertiser_id}})
                MATCH (lobbyist)-[:LOBBIES_FOR]->(client:Organization)
                WHERE client.id <> $advertiser_id
                  AND client.entity_type = 'ORGANIZATION'
                  {funder_filter}
                RETURN DISTINCT client.id as client_id,
                       client.name as client_name,
                       lobbyist.id as lobbyist_id,
                       lobbyist.name as lobbyist_name
                LIMIT 20
                """

                result = await session.run(query, **params)
                records = await result.data()

                if not records:
                    return None

                unique_clients = set(
                    r["client_id"] for r in records if r.get("client_id")
                )
                unique_lobbyists = set(
                    r["lobbyist_id"] for r in records if r.get("lobbyist_id")
                )

                # Strength based on number of shared clients
                strength = min(1.0, len(unique_clients) / 3.0)

                client_entity_ids = [UUID(cid) for cid in unique_clients]

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.LOBBYING_CLIENT,
                    strength=strength,
                    confidence=0.75,  # Moderate-high confidence - registry data
                    entity_ids=client_entity_ids,
                    metadata={
                        "client_count": len(unique_clients),
                        "lobbyist_count": len(unique_lobbyists),
                    },
                )

        except Exception as e:
            logger.warning(f"Error checking lobbying clients: {e}")
            return None

    async def _check_shared_directors(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if corporations share directors with TPA."""
        try:
            async with get_neo4j_session() as session:
                if funder_ids:
                    funder_filter = "AND corp.id IN $funder_ids"
                    params = {
                        "advertiser_id": str(advertiser_id),
                        "funder_ids": [str(fid) for fid in funder_ids],
                    }
                else:
                    funder_filter = ""
                    params = {"advertiser_id": str(advertiser_id)}

                query = f"""
                MATCH (director:Person)-[:DIRECTOR_OF]->(tpa:Organization {{id: $advertiser_id}})
                MATCH (director)-[:DIRECTOR_OF]->(corp:Organization)
                WHERE corp.id <> $advertiser_id
                  AND corp.entity_type = 'ORGANIZATION'
                  {funder_filter}
                RETURN DISTINCT corp.id as corp_id,
                       corp.name as corp_name,
                       director.id as director_id,
                       director.name as director_name
                LIMIT 20
                """

                result = await session.run(query, **params)
                records = await result.data()

                if not records:
                    return None

                unique_corps = set(r["corp_id"] for r in records if r.get("corp_id"))
                unique_directors = set(
                    r["director_id"] for r in records if r.get("director_id")
                )

                # Strength based on number of shared directors
                strength = min(1.0, len(unique_directors) / 3.0)

                corp_entity_ids = [UUID(cid) for cid in unique_corps]

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.SHARED_DIRECTORS,
                    strength=strength,
                    confidence=0.80,  # High confidence - official records
                    entity_ids=corp_entity_ids,
                    metadata={
                        "director_count": len(unique_directors),
                        "corporation_count": len(unique_corps),
                    },
                )

        except Exception as e:
            logger.warning(f"Error checking shared directors: {e}")
            return None

    async def _check_shared_address(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if corporations share registered office address with TPA."""
        try:
            async with get_db_session() as db:
                from sqlalchemy import text

                if funder_ids:
                    funder_filter = "AND e2.id = ANY(:funder_ids)"
                    params = {
                        "advertiser_id": advertiser_id,
                        "funder_ids": [str(fid) for fid in funder_ids],
                    }
                else:
                    funder_filter = ""
                    params = {"advertiser_id": advertiser_id}

                query = f"""
                SELECT DISTINCT e2.id as funder_id, e2.name as funder_name
                FROM entities e1
                JOIN entities e2 ON e1.registered_address = e2.registered_address
                WHERE e1.id = :advertiser_id
                  AND e1.registered_address IS NOT NULL
                  AND e1.registered_address != ''
                  AND e2.id <> :advertiser_id
                  AND e2.entity_type = 'ORGANIZATION'
                  {funder_filter}
                LIMIT 20
                """

                result = await db.execute(text(query), params)
                records = result.fetchall()

                if not records:
                    return None

                funder_entity_ids = [UUID(row[0]) for row in records]

                # Strength based on number of entities sharing address
                strength = min(1.0, len(funder_entity_ids) / 5.0)

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.SHARED_ADDRESS,
                    strength=strength,
                    confidence=0.70,  # Moderate confidence - could be co-location
                    entity_ids=funder_entity_ids,
                    metadata={"shared_address_count": len(funder_entity_ids)},
                )

        except Exception as e:
            logger.warning(f"Error checking shared address: {e}")
            return None

    async def _check_ppsa(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if corporations have PPSA secured interests with TPA."""
        try:
            async with get_neo4j_session() as session:
                if funder_ids:
                    funder_filter = "AND creditor.id IN $funder_ids"
                    params = {
                        "advertiser_id": str(advertiser_id),
                        "funder_ids": [str(fid) for fid in funder_ids],
                    }
                else:
                    funder_filter = ""
                    params = {"advertiser_id": str(advertiser_id)}

                query = f"""
                MATCH (debtor:Organization {{id: $advertiser_id}})-[:SECURED_BY]->(creditor:Organization)
                WHERE creditor.entity_type = 'ORGANIZATION'
                  {funder_filter}
                RETURN DISTINCT creditor.id as creditor_id,
                       creditor.name as creditor_name
                LIMIT 20
                """

                result = await session.run(query, **params)
                records = await result.data()

                if not records:
                    return None

                creditor_entity_ids = [
                    UUID(r["creditor_id"]) for r in records if r.get("creditor_id")
                ]

                # Strength based on number of secured interests
                strength = min(1.0, len(creditor_entity_ids) / 3.0)

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.PPSA_SECURED_INTEREST,
                    strength=strength,
                    confidence=0.75,  # Moderate-high confidence - official registry
                    entity_ids=creditor_entity_ids,
                    metadata={"creditor_count": len(creditor_entity_ids)},
                )

        except Exception as e:
            logger.warning(f"Error checking PPSA: {e}")
            return None

    async def _check_shared_agent(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if corporations share incorporation agent/lawyer with TPA."""
        try:
            async with get_db_session() as db:
                from sqlalchemy import text

                if funder_ids:
                    funder_filter = "AND e2.id = ANY(:funder_ids)"
                    params = {
                        "advertiser_id": advertiser_id,
                        "funder_ids": [str(fid) for fid in funder_ids],
                    }
                else:
                    funder_filter = ""
                    params = {"advertiser_id": advertiser_id}

                query = f"""
                SELECT DISTINCT e2.id as funder_id, e2.name as funder_name
                FROM entities e1
                JOIN entities e2 ON (
                    e1.incorporation_agent = e2.incorporation_agent
                    OR e1.incorporation_lawyer = e2.incorporation_lawyer
                )
                WHERE e1.id = :advertiser_id
                  AND (e1.incorporation_agent IS NOT NULL OR e1.incorporation_lawyer IS NOT NULL)
                  AND e2.id <> :advertiser_id
                  AND e2.entity_type = 'ORGANIZATION'
                  {funder_filter}
                LIMIT 20
                """

                result = await db.execute(text(query), params)
                records = result.fetchall()

                if not records:
                    return None

                funder_entity_ids = [UUID(row[0]) for row in records]

                # Lower strength - shared agent is common
                strength = min(0.6, len(funder_entity_ids) / 10.0)

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.SHARED_AGENT,
                    strength=strength,
                    confidence=0.50,  # Lower confidence - common practice
                    entity_ids=funder_entity_ids,
                    metadata={"shared_agent_count": len(funder_entity_ids)},
                )

        except Exception as e:
            logger.warning(f"Error checking shared agent: {e}")
            return None

    async def _check_shell_heuristic(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if TPA has no other public clients (shell company heuristic)."""
        try:
            async with get_neo4j_session() as session:
                # Check for other clients (lobbying, contributions, etc.)
                query = """
                MATCH (tpa:Organization {id: $advertiser_id})
                OPTIONAL MATCH (other)-[:CONTRIBUTED_TO]->(tpa)
                WHERE other.id <> $advertiser_id
                WITH tpa, count(DISTINCT other) as contributor_count
                
                OPTIONAL MATCH (lobbyist)-[:LOBBIES_FOR]->(tpa)
                WITH tpa, contributor_count, count(DISTINCT lobbyist) as lobbyist_count
                
                OPTIONAL MATCH (client)-[:LOBBIES_FOR]->(lobbyist)-[:LOBBIES_FOR]->(tpa)
                WHERE client.id <> $advertiser_id
                WITH tpa, contributor_count, lobbyist_count, count(DISTINCT client) as client_count
                
                RETURN contributor_count, lobbyist_count, client_count
                """

                result = await session.run(
                    query, advertiser_id=str(advertiser_id)
                )
                record = await result.single()

                if not record:
                    return None

                contributor_count = record.get("contributor_count", 0) or 0
                lobbyist_count = record.get("lobbyist_count", 0) or 0
                client_count = record.get("client_count", 0) or 0

                total_public_connections = (
                    contributor_count + lobbyist_count + client_count
                )

                # Flag if very few public connections (potential shell)
                if total_public_connections <= 1:
                    strength = 0.8
                elif total_public_connections <= 3:
                    strength = 0.5
                else:
                    return None  # Not a shell company

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.SHELL_HEURISTIC,
                    strength=strength,
                    confidence=0.60,  # Moderate confidence - heuristic
                    entity_ids=[advertiser_id],
                    metadata={
                        "contributor_count": contributor_count,
                        "lobbyist_count": lobbyist_count,
                        "client_count": client_count,
                        "total_public_connections": total_public_connections,
                    },
                )

        except Exception as e:
            logger.warning(f"Error checking shell heuristic: {e}")
            return None

    async def _check_temporal_correlation(
        self,
        advertiser_id: UUID,
        funder_ids: list[UUID] | None = None,
    ) -> PoliticalFundingSignal | None:
        """Check if corporations were founded shortly before ad campaign."""
        try:
            async with get_db_session() as db:
                from sqlalchemy import text

                # Get TPA's first ad campaign date (if available)
                # For now, use a heuristic based on entity creation date
                tpa_query = """
                SELECT created_at, incorporation_date
                FROM entities
                WHERE id = :advertiser_id
                """

                tpa_result = await db.execute(
                    text(tpa_query), {"advertiser_id": advertiser_id}
                )
                tpa_row = tpa_result.fetchone()

                if not tpa_row:
                    return None

                # Use incorporation_date or created_at as proxy for campaign start
                campaign_start = tpa_row[1] or tpa_row[0]
                if not campaign_start:
                    return None

                # Look for corporations founded within 6 months before campaign
                if isinstance(campaign_start, str):
                    from datetime import datetime

                    campaign_start = datetime.fromisoformat(campaign_start.replace("Z", "+00:00"))

                six_months_before = campaign_start - timedelta(days=180)

                if funder_ids:
                    funder_filter = "AND e.id = ANY(:funder_ids)"
                    params = {
                        "advertiser_id": advertiser_id,
                        "campaign_start": campaign_start,
                        "six_months_before": six_months_before,
                        "funder_ids": [str(fid) for fid in funder_ids],
                    }
                else:
                    funder_filter = ""
                    params = {
                        "advertiser_id": advertiser_id,
                        "campaign_start": campaign_start,
                        "six_months_before": six_months_before,
                    }

                query = f"""
                SELECT e.id as funder_id, e.name as funder_name, e.incorporation_date
                FROM entities e
                WHERE e.id <> :advertiser_id
                  AND e.entity_type = 'ORGANIZATION'
                  AND e.incorporation_date IS NOT NULL
                  AND e.incorporation_date >= :six_months_before
                  AND e.incorporation_date < :campaign_start
                  {funder_filter}
                LIMIT 20
                """

                result = await db.execute(text(query), params)
                records = result.fetchall()

                if not records:
                    return None

                funder_entity_ids = [UUID(row[0]) for row in records]

                # Strength based on number of recently founded corporations
                strength = min(1.0, len(funder_entity_ids) / 3.0)

                return PoliticalFundingSignal(
                    signal_type=PoliticalFundingSignalType.TEMPORAL_CORRELATION,
                    strength=strength,
                    confidence=0.65,  # Moderate confidence - correlation not causation
                    entity_ids=funder_entity_ids,
                    metadata={
                        "corporation_count": len(funder_entity_ids),
                        "campaign_start": campaign_start.isoformat(),
                        "window_days": 180,
                    },
                )

        except Exception as e:
            logger.warning(f"Error checking temporal correlation: {e}")
            return None
