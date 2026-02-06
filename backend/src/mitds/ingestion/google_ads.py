"""Google Political Ads ingester via BigQuery.

Queries the public BigQuery dataset `bigquery-public-data.google_political_ads`
for Canadian political ads. Creates SPONSORED_BY relationships in Neo4j.

Data source:
- BigQuery dataset: bigquery-public-data.google_political_ads
- Key tables: creative_stats, advertiser_stats, advertiser_weekly_spend, geo_spend
- Filter: WHERE regions = 'CA' for Canadian political ads
- Auth: Google Cloud account (free tier: 1 TB/month queries)

Key limitations:
- No "paid for by" field in BigQuery — disclaimer is on ad creative only
- No ad creative content — ad_type available but not actual images/text/video
- Impressions are bucketed ranges, not exact counts
- Coverage: August 2018 onward for Canada
"""

import asyncio
from datetime import date, datetime
from decimal import Decimal
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from .base import BaseIngester, IngestionConfig, IngestionResult, Neo4jHelper

logger = get_context_logger(__name__)


class GooglePoliticalAdRecord(BaseModel):
    """Record for a Google political ad from BigQuery."""

    ad_id: str = Field(..., description="Google's unique ad identifier")
    ad_url: str | None = Field(default=None, description="Link to Transparency Center")
    ad_type: str | None = Field(default=None, description="TEXT, IMAGE, or VIDEO")
    advertiser_id: str | None = Field(default=None, description="Google advertiser identifier")
    advertiser_name: str | None = Field(default=None, description="Advertiser display name")
    date_range_start: date | None = Field(default=None, description="When ad started running")
    date_range_end: date | None = Field(default=None, description="When ad stopped")
    spend_range_min_cad: Decimal | None = Field(
        default=None, description="Minimum spend in CAD"
    )
    spend_range_max_cad: Decimal | None = Field(
        default=None, description="Maximum spend in CAD"
    )
    impressions_bucket: str | None = Field(
        default=None, description="Bucketed range (e.g., '1000-2000')"
    )
    age_targeting: str | None = Field(default=None, description="Target demographics")
    gender_targeting: str | None = Field(default=None, description="Target demographics")
    geo_targeting: str | None = Field(default=None, description="Target geography")
    regions: str | None = Field(default=None, description="Region code (e.g., 'CA')")


class GooglePoliticalAdsIngester(BaseIngester["GooglePoliticalAdRecord"]):
    """Ingester for Google political ads via BigQuery public dataset.

    Queries BigQuery for Canadian political ads and stores them as
    ad entities with SPONSORED_BY relationships to advertisers.
    """

    def __init__(self) -> None:
        super().__init__("google_political_ads")

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[GooglePoliticalAdRecord]:
        """Fetch political ad records from BigQuery.

        Config options:
        - country: Country code (default: 'CA')
        - advertiser_name: Filter by advertiser
        - date_from: Start date filter
        - date_to: End date filter
        - limit: Max records
        """
        country = config.extra.get("country", "CA")
        advertiser_name = config.extra.get("advertiser_name")
        date_from = config.extra.get("date_from")
        date_to = config.extra.get("date_to")
        limit = config.extra.get("limit", 100)

        settings = get_settings()

        try:
            from google.cloud import bigquery

            # Set up BigQuery client
            # Uses GOOGLE_APPLICATION_CREDENTIALS env var or Application Default Credentials
            client = bigquery.Client()

            query = self._build_query(
                country=country,
                advertiser_name=advertiser_name,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
            )

            logger.info(f"Querying BigQuery for {country} political ads...")
            query_job = client.query(query)
            results = query_job.result()

            for row in results:
                yield GooglePoliticalAdRecord(
                    ad_id=row.get("ad_id", ""),
                    ad_url=row.get("ad_url"),
                    ad_type=row.get("ad_type"),
                    advertiser_id=row.get("advertiser_id"),
                    advertiser_name=row.get("advertiser_name"),
                    date_range_start=row.get("date_range_start"),
                    date_range_end=row.get("date_range_end"),
                    spend_range_min_cad=Decimal(str(row["spend_range_min_cad"]))
                    if row.get("spend_range_min_cad") is not None
                    else None,
                    spend_range_max_cad=Decimal(str(row["spend_range_max_cad"]))
                    if row.get("spend_range_max_cad") is not None
                    else None,
                    impressions_bucket=row.get("num_of_days"),
                    age_targeting=row.get("age_targeting"),
                    gender_targeting=row.get("gender_targeting"),
                    geo_targeting=row.get("geo_targeting_included"),
                    regions=row.get("regions"),
                )

        except ImportError:
            logger.error(
                "google-cloud-bigquery is not installed. "
                "Install with: pip install google-cloud-bigquery"
            )
        except Exception as e:
            logger.error(f"BigQuery query failed: {e}")
            raise

    async def process_record(self, record: GooglePoliticalAdRecord) -> dict[str, Any]:
        """Process a Google political ad record.

        Creates ad entity and SPONSORED_BY relationship to advertiser.
        """
        entity_id = uuid4()
        result: dict[str, Any] = {"created": False, "entity_id": str(entity_id)}

        # Store ad entity in PostgreSQL
        async with get_db_session() as db:
            from sqlalchemy import text

            # Check if ad already exists
            existing = await db.execute(
                text(
                    """
                    SELECT id FROM entities
                    WHERE external_ids->>'google_ad_id' = :ad_id
                    LIMIT 1
                    """
                ),
                {"ad_id": record.ad_id},
            )
            existing_row = existing.fetchone()

            if existing_row:
                entity_id = existing_row[0]
                # Update existing record
                await db.execute(
                    text(
                        """
                        UPDATE entities SET
                            metadata = metadata || :metadata,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": str(entity_id),
                        "metadata": {
                            "ad_type": record.ad_type,
                            "spend_range_min_cad": str(record.spend_range_min_cad)
                            if record.spend_range_min_cad
                            else None,
                            "spend_range_max_cad": str(record.spend_range_max_cad)
                            if record.spend_range_max_cad
                            else None,
                            "date_range_start": str(record.date_range_start)
                            if record.date_range_start
                            else None,
                            "date_range_end": str(record.date_range_end)
                            if record.date_range_end
                            else None,
                        },
                    },
                )
                result = {"updated": True, "entity_id": str(entity_id)}
            else:
                await db.execute(
                    text(
                        """
                        INSERT INTO entities (
                            id, name, entity_type, jurisdiction,
                            external_ids, metadata, created_at, updated_at
                        )
                        VALUES (
                            :id, :name, 'ad', 'CA',
                            :external_ids, :metadata, NOW(), NOW()
                        )
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        "id": str(entity_id),
                        "name": f"Google Ad {record.ad_id}",
                        "external_ids": {"google_ad_id": record.ad_id},
                        "metadata": {
                            "ad_url": record.ad_url,
                            "ad_type": record.ad_type,
                            "advertiser_id": record.advertiser_id,
                            "advertiser_name": record.advertiser_name,
                            "spend_range_min_cad": str(record.spend_range_min_cad)
                            if record.spend_range_min_cad
                            else None,
                            "spend_range_max_cad": str(record.spend_range_max_cad)
                            if record.spend_range_max_cad
                            else None,
                            "impressions_bucket": record.impressions_bucket,
                            "age_targeting": record.age_targeting,
                            "gender_targeting": record.gender_targeting,
                            "geo_targeting": record.geo_targeting,
                            "platform": "google",
                        },
                    },
                )
                result = {"created": True, "entity_id": str(entity_id)}

        # Create SPONSORED_BY relationship in Neo4j
        try:
            async with get_neo4j_session() as session:
                await session.run(
                    """
                    MERGE (ad:Ad {google_ad_id: $ad_id})
                    ON CREATE SET ad.id = $entity_id,
                                  ad.name = $ad_name,
                                  ad.platform = 'google',
                                  ad.ad_type = $ad_type,
                                  ad.created_at = datetime()
                    SET ad.updated_at = datetime(),
                        ad.spend_min_cad = $spend_min,
                        ad.spend_max_cad = $spend_max

                    WITH ad
                    MERGE (sponsor:Organization {name: $advertiser_name})
                    ON CREATE SET sponsor.id = $sponsor_id,
                                  sponsor.created_at = datetime(),
                                  sponsor.source = 'google_ads'

                    MERGE (ad)-[r:SPONSORED_BY]->(sponsor)
                    SET r.platform = 'google',
                        r.spend_min_cad = $spend_min,
                        r.spend_max_cad = $spend_max,
                        r.source = 'google_ads',
                        r.updated_at = datetime()
                    """,
                    ad_id=record.ad_id,
                    entity_id=str(entity_id),
                    ad_name=f"Google Ad {record.ad_id}",
                    ad_type=record.ad_type,
                    spend_min=float(record.spend_range_min_cad)
                    if record.spend_range_min_cad
                    else None,
                    spend_max=float(record.spend_range_max_cad)
                    if record.spend_range_max_cad
                    else None,
                    advertiser_name=record.advertiser_name or f"Advertiser {record.advertiser_id}",
                    sponsor_id=str(uuid4()),
                )
        except Exception as e:
            logger.warning(f"Neo4j sync failed for Google ad {record.ad_id}: {e}")

        return result

    async def get_last_sync_time(self) -> datetime | None:
        """Get last sync timestamp."""
        async with get_db_session() as db:
            from sqlalchemy import text

            result = await db.execute(
                text(
                    "SELECT MAX(completed_at) FROM ingestion_runs WHERE source = 'google_political_ads'"
                )
            )
            row = result.fetchone()
            return row[0] if row and row[0] else None

    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save sync timestamp — handled by base class."""
        pass

    def _build_query(
        self,
        country: str = "CA",
        advertiser_name: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 100,
    ) -> str:
        """Build BigQuery SQL for political ads."""
        query = """
            SELECT
                ad_id,
                ad_url,
                ad_type,
                advertiser_id,
                advertiser_name,
                date_range_start,
                date_range_end,
                spend_range_min_cad,
                spend_range_max_cad,
                num_of_days,
                age_targeting,
                gender_targeting,
                geo_targeting_included,
                regions
            FROM `bigquery-public-data.google_political_ads.creative_stats`
            WHERE regions = @country
        """

        params = [f"  AND regions = '{country}'"]

        if advertiser_name:
            query += f"\n  AND LOWER(advertiser_name) LIKE '%{advertiser_name.lower()}%'"

        if date_from:
            query += f"\n  AND date_range_start >= '{date_from}'"

        if date_to:
            query += f"\n  AND date_range_end <= '{date_to}'"

        query += f"\n  ORDER BY date_range_start DESC"
        query += f"\n  LIMIT {limit}"

        return query


async def run_google_ads_ingestion(
    country: str = "CA",
    advertiser_name: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 100,
    verbose: bool = False,
) -> IngestionResult:
    """Run the Google Political Ads ingestion pipeline.

    Args:
        country: Country code (default: 'CA')
        advertiser_name: Filter by advertiser name
        date_from: Start date filter
        date_to: End date filter
        limit: Maximum ads to process
        verbose: Enable verbose output
    """
    ingester = GooglePoliticalAdsIngester()
    config = IngestionConfig(
        incremental=True,
        extra={
            "country": country,
            "advertiser_name": advertiser_name,
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
        },
    )
    run_id = uuid4()
    return await ingester.run(config, run_id)


async def detect_cross_platform_disclaimers() -> list[dict[str, Any]]:
    """Detect discrepancies in advertiser names across Google and Meta platforms.

    Queries Neo4j for entities (ads) with both Google SPONSORED_BY and Meta
    SPONSORED_BY relationships, then compares advertiser names using fuzzy matching
    to flag cases where names are similar but different (potential discrepancies).

    Returns:
        List of discrepancy dictionaries with:
        - google_advertiser: Advertiser name from Google
        - meta_sponsor: Sponsor name from Meta
        - similarity_score: Fuzzy match score (0.5-0.85 range)
        - platforms: List of platforms involved
        - example_google_ad: Example Google ad entity name
        - example_google_ad_id: Example Google ad ID
        - example_meta_ad: Example Meta ad entity name
        - example_meta_ad_id: Example Meta ad ID
    """
    try:
        from rapidfuzz import fuzz

        discrepancies: list[dict[str, Any]] = []

        async with get_neo4j_session() as session:
            # Query for all Google advertisers and Meta sponsors with example ad entities
            query = """
            MATCH (google_ad:Ad)-[r1:SPONSORED_BY]->(google_sponsor:Organization)
            WHERE r1.platform = 'google'
            WITH google_sponsor.name AS google_name, 
                 collect(DISTINCT google_ad.name)[0] AS example_google_ad,
                 collect(DISTINCT google_ad.google_ad_id)[0] AS example_google_ad_id
            MATCH (meta_ad:Ad)-[r2:SPONSORED_BY]->(meta_sponsor:Sponsor)
            WHERE meta_ad.meta_ad_id IS NOT NULL
            WITH google_name, example_google_ad, example_google_ad_id,
                 meta_sponsor.name AS meta_name,
                 collect(DISTINCT meta_ad.name)[0] AS example_meta_ad,
                 collect(DISTINCT meta_ad.meta_ad_id)[0] AS example_meta_ad_id
            WHERE google_name IS NOT NULL AND meta_name IS NOT NULL
            RETURN DISTINCT google_name, meta_name,
                   example_google_ad, example_google_ad_id,
                   example_meta_ad, example_meta_ad_id
            """
            result = await session.run(query)
            records = await result.data()

            # Compare all pairs and flag discrepancies
            for record in records:
                google_name = record.get("google_name", "")
                meta_name = record.get("meta_name", "")

                if not google_name or not meta_name:
                    continue

                # Calculate fuzzy match score
                similarity_score = fuzz.ratio(google_name.lower(), meta_name.lower()) / 100.0

                # Flag cases where similarity is between 0.5-0.85 (similar but different)
                if 0.5 <= similarity_score <= 0.85:
                    discrepancies.append({
                        "google_advertiser": google_name,
                        "meta_sponsor": meta_name,
                        "similarity_score": similarity_score,
                        "platforms": ["google", "meta"],
                        "example_google_ad": record.get("example_google_ad"),
                        "example_google_ad_id": record.get("example_google_ad_id"),
                        "example_meta_ad": record.get("example_meta_ad"),
                        "example_meta_ad_id": record.get("example_meta_ad_id"),
                    })

        logger.info(
            f"Detected {len(discrepancies)} cross-platform advertiser name discrepancies"
        )
        return discrepancies

    except ImportError:
        logger.error(
            "rapidfuzz is not installed. "
            "Install with: pip install rapidfuzz"
        )
        return []
    except Exception as e:
        logger.error(f"Cross-platform disclaimer detection failed: {e}")
        return []
