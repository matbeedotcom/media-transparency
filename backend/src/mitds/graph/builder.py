"""Graph builder for Neo4j operations.

Handles creation and updating of nodes and relationships in the
Neo4j graph database.
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel

from ..db import get_neo4j_session
from ..logging import get_context_logger
from ..models import (
    EntityType,
    MediaType,
    Organization,
    OrgStatus,
    OrgType,
    Outlet,
    Person,
    Sponsor,
)
from ..models.relationships import RelationType

logger = get_context_logger(__name__)


class NodeResult(BaseModel):
    """Result of a node operation."""

    id: UUID
    entity_type: str
    created: bool
    updated: bool


class RelationshipResult(BaseModel):
    """Result of a relationship operation."""

    id: UUID
    rel_type: str
    source_id: UUID
    target_id: UUID
    created: bool
    updated: bool


class GraphBuilder:
    """Builder for creating and managing entities in Neo4j.

    Provides methods to:
    - Create/update entity nodes
    - Create/update relationships
    - Batch operations for efficiency
    """

    async def create_organization(
        self,
        name: str,
        org_type: OrgType = OrgType.UNKNOWN,
        status: OrgStatus = OrgStatus.UNKNOWN,
        jurisdiction: str = "US",
        ein: str | None = None,
        bn: str | None = None,
        opencorp_id: str | None = None,
        address: dict[str, str] | None = None,
        confidence: float = 1.0,
        entity_id: UUID | None = None,
    ) -> NodeResult:
        """Create or update an organization node.

        Args:
            name: Organization name
            org_type: Type of organization
            status: Operational status
            jurisdiction: Country or state/province
            ein: US EIN (optional)
            bn: Canadian BN (optional)
            opencorp_id: OpenCorporates ID (optional)
            address: Address dictionary (optional)
            confidence: Confidence score
            entity_id: Existing entity ID for updates

        Returns:
            NodeResult with operation status
        """
        if entity_id is None:
            entity_id = uuid4()

        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()

            # Build properties
            props = {
                "id": str(entity_id),
                "name": name,
                "entity_type": EntityType.ORGANIZATION.value,
                "org_type": org_type.value,
                "status": status.value,
                "jurisdiction": jurisdiction,
                "confidence": confidence,
                "updated_at": now,
            }

            if ein:
                props["ein"] = ein
            if bn:
                props["bn"] = bn
            if opencorp_id:
                props["opencorp_id"] = opencorp_id
            if address:
                if address.get("street"):
                    props["address_street"] = address["street"]
                if address.get("city"):
                    props["address_city"] = address["city"]
                if address.get("state"):
                    props["address_state"] = address["state"]
                if address.get("postal_code"):
                    props["address_postal"] = address["postal_code"]
                if address.get("country"):
                    props["address_country"] = address["country"]

            # Determine merge key
            if ein:
                merge_key = "ein"
                merge_value = ein
            elif bn:
                merge_key = "bn"
                merge_value = bn
            else:
                merge_key = "id"
                merge_value = str(entity_id)

            # Check if exists
            check_query = f"""
            MATCH (o:Organization {{{merge_key}: $merge_value}})
            RETURN o.id as id
            """
            result = await session.run(check_query, merge_value=merge_value)
            existing = await result.single()

            created = existing is None

            if created:
                props["created_at"] = now

            # Upsert query
            query = f"""
            MERGE (o:Organization {{{merge_key}: $merge_value}})
            SET o += $props
            RETURN o.id as id
            """
            result = await session.run(
                query, merge_value=merge_value, props=props
            )
            record = await result.single()

            return NodeResult(
                id=UUID(record["id"]),
                entity_type=EntityType.ORGANIZATION.value,
                created=created,
                updated=not created,
            )

    async def create_person(
        self,
        name: str,
        aliases: list[str] | None = None,
        irs_990_name: str | None = None,
        opencorp_officer_id: str | None = None,
        location: str | None = None,
        confidence: float = 1.0,
        entity_id: UUID | None = None,
    ) -> NodeResult:
        """Create or update a person node.

        Args:
            name: Person's name
            aliases: Known alternate names
            irs_990_name: Name as appears in IRS 990
            opencorp_officer_id: OpenCorporates officer ID
            location: City, State/Province
            confidence: Confidence score
            entity_id: Existing entity ID for updates

        Returns:
            NodeResult with operation status
        """
        if entity_id is None:
            entity_id = uuid4()

        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()

            props = {
                "id": str(entity_id),
                "name": name,
                "entity_type": EntityType.PERSON.value,
                "confidence": confidence,
                "updated_at": now,
            }

            if aliases:
                props["aliases"] = aliases
            if irs_990_name:
                props["irs_990_name"] = irs_990_name
            if opencorp_officer_id:
                props["opencorp_officer_id"] = opencorp_officer_id
            if location:
                props["location"] = location

            # Determine merge key
            if irs_990_name:
                merge_key = "irs_990_name"
                merge_value = irs_990_name
            elif opencorp_officer_id:
                merge_key = "opencorp_officer_id"
                merge_value = opencorp_officer_id
            else:
                merge_key = "name"
                merge_value = name

            # Check if exists
            check_query = f"""
            MATCH (p:Person {{{merge_key}: $merge_value}})
            RETURN p.id as id
            """
            result = await session.run(check_query, merge_value=merge_value)
            existing = await result.single()

            created = existing is None

            if created:
                props["created_at"] = now

            # Upsert query
            query = f"""
            MERGE (p:Person {{{merge_key}: $merge_value}})
            SET p += $props
            RETURN p.id as id
            """
            result = await session.run(
                query, merge_value=merge_value, props=props
            )
            record = await result.single()

            return NodeResult(
                id=UUID(record["id"]),
                entity_type=EntityType.PERSON.value,
                created=created,
                updated=not created,
            )

    async def create_outlet(
        self,
        name: str,
        aliases: list[str] | None = None,
        domains: list[str] | None = None,
        media_type: MediaType = MediaType.DIGITAL,
        editorial_focus: list[str] | None = None,
        owner_org_id: UUID | None = None,
        confidence: float = 1.0,
        entity_id: UUID | None = None,
    ) -> NodeResult:
        """Create or update an outlet node.

        Args:
            name: Outlet name
            aliases: Former/alternate names
            domains: Associated domains
            media_type: Type of media
            editorial_focus: Topic tags
            owner_org_id: Owning organization ID
            confidence: Confidence score
            entity_id: Existing entity ID for updates

        Returns:
            NodeResult with operation status
        """
        if entity_id is None:
            entity_id = uuid4()

        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()

            props = {
                "id": str(entity_id),
                "name": name,
                "entity_type": EntityType.OUTLET.value,
                "media_type": media_type.value,
                "confidence": confidence,
                "updated_at": now,
            }

            if aliases:
                props["aliases"] = aliases
            if domains:
                props["domains"] = domains
            if editorial_focus:
                props["editorial_focus"] = editorial_focus
            if owner_org_id:
                props["owner_org_id"] = str(owner_org_id)

            # Check if exists
            check_query = """
            MATCH (o:Outlet {name: $name})
            RETURN o.id as id
            """
            result = await session.run(check_query, name=name)
            existing = await result.single()

            created = existing is None

            if created:
                props["created_at"] = now

            # Upsert query
            query = """
            MERGE (o:Outlet {name: $name})
            SET o += $props
            RETURN o.id as id
            """
            result = await session.run(query, name=name, props=props)
            record = await result.single()

            return NodeResult(
                id=UUID(record["id"]),
                entity_type=EntityType.OUTLET.value,
                created=created,
                updated=not created,
            )

    async def create_sponsor(
        self,
        name: str,
        resolved_org_id: UUID | None = None,
        meta_page_id: str | None = None,
        meta_disclaimer_text: str | None = None,
        confidence: float = 0.8,
        entity_id: UUID | None = None,
    ) -> NodeResult:
        """Create or update a sponsor node.

        Args:
            name: Sponsor name
            resolved_org_id: Link to Organization if resolved
            meta_page_id: Meta/Facebook Page ID
            meta_disclaimer_text: Original disclaimer text
            confidence: Confidence score
            entity_id: Existing entity ID for updates

        Returns:
            NodeResult with operation status
        """
        if entity_id is None:
            entity_id = uuid4()

        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()

            props = {
                "id": str(entity_id),
                "name": name,
                "entity_type": EntityType.SPONSOR.value,
                "confidence": confidence,
                "updated_at": now,
            }

            if resolved_org_id:
                props["resolved_org_id"] = str(resolved_org_id)
            if meta_page_id:
                props["meta_page_id"] = meta_page_id
            if meta_disclaimer_text:
                props["meta_disclaimer_text"] = meta_disclaimer_text

            # Check if exists
            check_query = """
            MATCH (s:Sponsor {name: $name})
            RETURN s.id as id
            """
            result = await session.run(check_query, name=name)
            existing = await result.single()

            created = existing is None

            if created:
                props["created_at"] = now

            # Upsert query
            query = """
            MERGE (s:Sponsor {name: $name})
            SET s += $props
            RETURN s.id as id
            """
            result = await session.run(query, name=name, props=props)
            record = await result.single()

            return NodeResult(
                id=UUID(record["id"]),
                entity_type=EntityType.SPONSOR.value,
                created=created,
                updated=not created,
            )

    async def create_funded_by_relationship(
        self,
        recipient_id: UUID,
        funder_id: UUID,
        amount: float | None = None,
        amount_currency: str = "USD",
        fiscal_year: int | None = None,
        grant_purpose: str | None = None,
        valid_from: datetime | None = None,
        valid_to: datetime | None = None,
        confidence: float = 1.0,
    ) -> RelationshipResult:
        """Create or update a FUNDED_BY relationship.

        Args:
            recipient_id: ID of entity receiving funding
            funder_id: ID of funding entity
            amount: Grant/funding amount
            amount_currency: Currency code
            fiscal_year: Fiscal year of funding
            grant_purpose: Purpose of grant
            valid_from: When relationship started
            valid_to: When relationship ended
            confidence: Confidence score

        Returns:
            RelationshipResult with operation status
        """
        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()
            rel_id = uuid4()

            props = {
                "id": str(rel_id),
                "confidence": confidence,
                "updated_at": now,
            }

            if amount is not None:
                props["amount"] = amount
            if amount_currency:
                props["amount_currency"] = amount_currency
            if fiscal_year:
                props["fiscal_year"] = fiscal_year
            if grant_purpose:
                props["grant_purpose"] = grant_purpose
            if valid_from:
                props["valid_from"] = valid_from.isoformat()
            if valid_to:
                props["valid_to"] = valid_to.isoformat()

            # Check if exists
            check_query = """
            MATCH (recipient {id: $recipient_id})-[r:FUNDED_BY]->(funder {id: $funder_id})
            WHERE r.fiscal_year = $fiscal_year OR ($fiscal_year IS NULL AND r.fiscal_year IS NULL)
            RETURN r.id as id
            """
            result = await session.run(
                check_query,
                recipient_id=str(recipient_id),
                funder_id=str(funder_id),
                fiscal_year=fiscal_year,
            )
            existing = await result.single()

            created = existing is None

            if not created:
                rel_id = UUID(existing["id"])
            else:
                props["created_at"] = now

            # Upsert relationship
            query = """
            MATCH (recipient {id: $recipient_id})
            MATCH (funder {id: $funder_id})
            MERGE (recipient)-[r:FUNDED_BY]->(funder)
            SET r += $props
            RETURN r.id as id
            """
            result = await session.run(
                query,
                recipient_id=str(recipient_id),
                funder_id=str(funder_id),
                props=props,
            )
            await result.single()

            return RelationshipResult(
                id=rel_id,
                rel_type=RelationType.FUNDED_BY.value,
                source_id=recipient_id,
                target_id=funder_id,
                created=created,
                updated=not created,
            )

    async def create_director_of_relationship(
        self,
        person_id: UUID,
        organization_id: UUID,
        title: str | None = None,
        compensation: float | None = None,
        hours_per_week: float | None = None,
        valid_from: datetime | None = None,
        valid_to: datetime | None = None,
        confidence: float = 1.0,
    ) -> RelationshipResult:
        """Create or update a DIRECTOR_OF relationship.

        Args:
            person_id: ID of person
            organization_id: ID of organization
            title: Position title
            compensation: Annual compensation
            hours_per_week: Hours per week devoted
            valid_from: When relationship started
            valid_to: When relationship ended
            confidence: Confidence score

        Returns:
            RelationshipResult with operation status
        """
        return await self._create_role_relationship(
            person_id=person_id,
            organization_id=organization_id,
            rel_type="DIRECTOR_OF",
            title=title,
            compensation=compensation,
            hours_per_week=hours_per_week,
            valid_from=valid_from,
            valid_to=valid_to,
            confidence=confidence,
        )

    async def create_employed_by_relationship(
        self,
        person_id: UUID,
        organization_id: UUID,
        title: str | None = None,
        compensation: float | None = None,
        hours_per_week: float | None = None,
        valid_from: datetime | None = None,
        valid_to: datetime | None = None,
        confidence: float = 1.0,
    ) -> RelationshipResult:
        """Create or update an EMPLOYED_BY relationship.

        Args:
            person_id: ID of person
            organization_id: ID of organization
            title: Position title
            compensation: Annual compensation
            hours_per_week: Hours per week devoted
            valid_from: When relationship started
            valid_to: When relationship ended
            confidence: Confidence score

        Returns:
            RelationshipResult with operation status
        """
        return await self._create_role_relationship(
            person_id=person_id,
            organization_id=organization_id,
            rel_type="EMPLOYED_BY",
            title=title,
            compensation=compensation,
            hours_per_week=hours_per_week,
            valid_from=valid_from,
            valid_to=valid_to,
            confidence=confidence,
        )

    async def create_owns_relationship(
        self,
        owner_id: UUID,
        owned_id: UUID,
        ownership_percentage: float | None = None,
        share_class: str | None = None,
        valid_from: datetime | None = None,
        valid_to: datetime | None = None,
        confidence: float = 1.0,
    ) -> RelationshipResult:
        """Create or update an OWNS relationship.

        Args:
            owner_id: ID of owner entity (Person or Organization)
            owned_id: ID of owned entity (Organization)
            ownership_percentage: Percentage ownership (0-100)
            share_class: Class of shares if applicable
            valid_from: When ownership started
            valid_to: When ownership ended
            confidence: Confidence score

        Returns:
            RelationshipResult with operation status
        """
        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()
            rel_id = uuid4()

            props = {
                "id": str(rel_id),
                "confidence": confidence,
                "updated_at": now,
            }

            if ownership_percentage is not None:
                props["ownership_percentage"] = ownership_percentage
            if share_class:
                props["share_class"] = share_class
            if valid_from:
                props["valid_from"] = valid_from.isoformat()
            if valid_to:
                props["valid_to"] = valid_to.isoformat()

            # Check if exists
            check_query = """
            MATCH (owner {id: $owner_id})-[r:OWNS]->(owned {id: $owned_id})
            RETURN r.id as id
            """
            result = await session.run(
                check_query,
                owner_id=str(owner_id),
                owned_id=str(owned_id),
            )
            existing = await result.single()

            created = existing is None

            if not created:
                rel_id = UUID(existing["id"])
            else:
                props["created_at"] = now

            # Upsert relationship
            query = """
            MATCH (owner {id: $owner_id})
            MATCH (owned {id: $owned_id})
            MERGE (owner)-[r:OWNS]->(owned)
            SET r += $props
            RETURN r.id as id
            """
            result = await session.run(
                query,
                owner_id=str(owner_id),
                owned_id=str(owned_id),
                props=props,
            )
            await result.single()

            return RelationshipResult(
                id=rel_id,
                rel_type="OWNS",
                source_id=owner_id,
                target_id=owned_id,
                created=created,
                updated=not created,
            )

    async def _create_role_relationship(
        self,
        person_id: UUID,
        organization_id: UUID,
        rel_type: str,
        title: str | None = None,
        compensation: float | None = None,
        hours_per_week: float | None = None,
        valid_from: datetime | None = None,
        valid_to: datetime | None = None,
        confidence: float = 1.0,
    ) -> RelationshipResult:
        """Create or update a role relationship (DIRECTOR_OF or EMPLOYED_BY)."""
        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()
            rel_id = uuid4()

            props = {
                "id": str(rel_id),
                "confidence": confidence,
                "updated_at": now,
            }

            if title:
                props["title"] = title
            if compensation is not None:
                props["compensation"] = compensation
            if hours_per_week is not None:
                props["hours_per_week"] = hours_per_week
            if valid_from:
                props["valid_from"] = valid_from.isoformat()
            if valid_to:
                props["valid_to"] = valid_to.isoformat()

            # Check if exists
            check_query = f"""
            MATCH (p:Person {{id: $person_id}})-[r:{rel_type}]->(o:Organization {{id: $org_id}})
            RETURN r.id as id
            """
            result = await session.run(
                check_query,
                person_id=str(person_id),
                org_id=str(organization_id),
            )
            existing = await result.single()

            created = existing is None

            if not created:
                rel_id = UUID(existing["id"])
            else:
                props["created_at"] = now

            # Upsert relationship
            query = f"""
            MATCH (p:Person {{id: $person_id}})
            MATCH (o:Organization {{id: $org_id}})
            MERGE (p)-[r:{rel_type}]->(o)
            SET r += $props
            RETURN r.id as id
            """
            result = await session.run(
                query,
                person_id=str(person_id),
                org_id=str(organization_id),
                props=props,
            )
            await result.single()

            return RelationshipResult(
                id=rel_id,
                rel_type=rel_type,
                source_id=person_id,
                target_id=organization_id,
                created=created,
                updated=not created,
            )

    async def create_shared_infra_relationship(
        self,
        source_id: UUID,
        target_id: UUID,
        confidence: float = 1.0,
        properties: dict | None = None,
    ) -> RelationshipResult:
        """Create or update a SHARED_INFRA relationship between outlets.

        Args:
            source_id: ID of first outlet
            target_id: ID of second outlet
            confidence: Confidence score (0.0-1.0)
            properties: Additional properties (shared_signals, sharing_category, etc.)

        Returns:
            RelationshipResult with operation status
        """
        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()
            rel_id = uuid4()

            props = {
                "id": str(rel_id),
                "confidence": confidence,
                "updated_at": now,
            }

            if properties:
                # Serialize complex types to JSON strings if needed
                import json
                for key, value in properties.items():
                    if isinstance(value, (list, dict)):
                        props[key] = json.dumps(value)
                    else:
                        props[key] = value

            # Check if exists (bidirectional check)
            check_query = """
            MATCH (a {id: $source_id})-[r:SHARED_INFRA]-(b {id: $target_id})
            RETURN r.id as id
            """
            result = await session.run(
                check_query,
                source_id=str(source_id),
                target_id=str(target_id),
            )
            existing = await result.single()

            created = existing is None

            if not created:
                rel_id = UUID(existing["id"])
            else:
                props["created_at"] = now

            # Upsert relationship
            query = """
            MATCH (a {id: $source_id})
            MATCH (b {id: $target_id})
            MERGE (a)-[r:SHARED_INFRA]-(b)
            SET r += $props
            RETURN r.id as id
            """
            result = await session.run(
                query,
                source_id=str(source_id),
                target_id=str(target_id),
                props=props,
            )
            await result.single()

            return RelationshipResult(
                id=rel_id,
                rel_type="SHARED_INFRA",
                source_id=source_id,
                target_id=target_id,
                created=created,
                updated=not created,
            )


# Singleton instance
_graph_builder: GraphBuilder | None = None


def get_graph_builder() -> GraphBuilder:
    """Get the graph builder singleton."""
    global _graph_builder
    if _graph_builder is None:
        _graph_builder = GraphBuilder()
    return _graph_builder
