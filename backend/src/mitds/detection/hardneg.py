"""Hard negative filtering for MITDS temporal detection.

Filters out legitimate explanations for synchronized timing:
1. Breaking news events (wire service coverage within short window)
2. Scheduled events (earnings, elections, press releases)
3. High-volume news days (>2σ above baseline)

These filters reduce false positives in coordination detection.
"""

from datetime import datetime, timedelta
from typing import Any

import httpx
from pydantic import BaseModel, Field

from ..config import get_settings
from ..logging import get_context_logger

logger = get_context_logger(__name__)


class HardNegativeEvent(BaseModel):
    """An event that explains synchronized timing legitimately."""

    event_type: str  # "breaking_news", "scheduled_event", "high_volume_day"
    event_time: datetime
    event_window_hours: int = 1
    source: str
    description: str
    confidence: float = Field(ge=0.0, le=1.0)


class HardNegativeFilter:
    """Base class for hard negative filters."""

    async def check_events(
        self,
        timestamps: list[datetime],
    ) -> list[HardNegativeEvent]:
        """Check if any timestamps fall within hard negative windows.

        Args:
            timestamps: List of event timestamps to check

        Returns:
            List of HardNegativeEvents that explain timing
        """
        raise NotImplementedError


class BreakingNewsFilter(HardNegativeFilter):
    """Filters events that coincide with major breaking news.

    Uses wire service detection: if AP, Reuters, or AFP published
    within 1 hour of the events, the timing is likely organic.
    """

    # Major wire services and their identifiable patterns
    WIRE_SERVICES = [
        "Associated Press",
        "AP News",
        "Reuters",
        "AFP",
        "Agence France-Presse",
        "Bloomberg",
        "United Press International",
        "UPI",
    ]

    def __init__(
        self,
        window_hours: int = 1,
        min_sources: int = 3,
    ):
        """Initialize the filter.

        Args:
            window_hours: Time window to check for breaking news
            min_sources: Minimum sources covering story to be "breaking"
        """
        self.window_hours = window_hours
        self.min_sources = min_sources
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get HTTP client for external API calls."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
            )
        return self._http_client

    async def close(self):
        """Close HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def check_events(
        self,
        timestamps: list[datetime],
    ) -> list[HardNegativeEvent]:
        """Check if timestamps coincide with breaking news.

        Note: In production, this would query a news API or internal
        database of wire service articles. For now, we check against
        a cache of known breaking news events.
        """
        if not timestamps:
            return []

        # Get time range to check
        min_time = min(timestamps) - timedelta(hours=self.window_hours)
        max_time = max(timestamps) + timedelta(hours=self.window_hours)

        # Query for breaking news in this window
        breaking_events = await self._query_breaking_news(min_time, max_time)

        # Check which timestamps fall within breaking news windows
        matches = []
        for event in breaking_events:
            event_start = event.event_time - timedelta(hours=self.window_hours / 2)
            event_end = event.event_time + timedelta(hours=self.window_hours / 2)

            affected_timestamps = [
                ts for ts in timestamps
                if event_start <= ts <= event_end
            ]

            if affected_timestamps:
                matches.append(event)

        return matches

    async def _query_breaking_news(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[HardNegativeEvent]:
        """Query for breaking news events in time range.

        In production, this would:
        1. Query a news API (e.g., NewsAPI, Event Registry)
        2. Check internal database of wire service articles
        3. Identify high-coverage stories

        For now, uses placeholder logic.
        """
        from ..db import get_db_session
        from sqlalchemy import text

        events = []

        try:
            async with get_db_session() as session:
                # Query events table for breaking news markers
                query = text("""
                    SELECT DISTINCT
                        event_time,
                        metadata->>'headline' as headline,
                        metadata->>'source' as source
                    FROM events
                    WHERE event_type = 'BREAKING_NEWS'
                    AND event_time BETWEEN :start AND :end
                    ORDER BY event_time
                """)

                result = await session.execute(
                    query,
                    {"start": start_time, "end": end_time},
                )
                rows = result.fetchall()

                for row in rows:
                    events.append(HardNegativeEvent(
                        event_type="breaking_news",
                        event_time=row.event_time,
                        event_window_hours=self.window_hours,
                        source=row.source or "wire_service",
                        description=row.headline or "Breaking news event",
                        confidence=0.9,
                    ))

        except Exception as e:
            logger.warning(f"Failed to query breaking news: {e}")

        return events


class ScheduledEventFilter(HardNegativeFilter):
    """Filters events that coincide with known scheduled events.

    Types of scheduled events:
    - Corporate earnings releases (quarterly)
    - Election dates
    - Government announcements (budget, policy)
    - Sports events
    - Entertainment releases
    """

    # Known recurring scheduled events
    SCHEDULED_EVENT_TYPES = {
        "earnings": {
            "description": "Corporate earnings release",
            "window_hours": 4,
        },
        "election": {
            "description": "Election day coverage",
            "window_hours": 24,
        },
        "budget": {
            "description": "Government budget announcement",
            "window_hours": 12,
        },
        "speech": {
            "description": "Major political speech",
            "window_hours": 4,
        },
        "product_launch": {
            "description": "Major product launch event",
            "window_hours": 6,
        },
    }

    def __init__(self):
        self._cached_events: dict[str, list[HardNegativeEvent]] = {}

    async def check_events(
        self,
        timestamps: list[datetime],
    ) -> list[HardNegativeEvent]:
        """Check if timestamps coincide with scheduled events."""
        if not timestamps:
            return []

        min_time = min(timestamps)
        max_time = max(timestamps)

        # Query scheduled events in range
        scheduled = await self._query_scheduled_events(min_time, max_time)

        matches = []
        for event in scheduled:
            window_hours = self.SCHEDULED_EVENT_TYPES.get(
                event.source, {}
            ).get("window_hours", 4)

            event_start = event.event_time - timedelta(hours=window_hours / 2)
            event_end = event.event_time + timedelta(hours=window_hours / 2)

            affected = [ts for ts in timestamps if event_start <= ts <= event_end]
            if affected:
                matches.append(event)

        return matches

    async def _query_scheduled_events(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[HardNegativeEvent]:
        """Query for scheduled events in time range.

        Sources:
        - SEC EDGAR for earnings (Form 8-K filings)
        - Election calendars
        - Government announcement schedules
        - PR Newswire/Business Wire for press releases
        """
        from ..db import get_db_session
        from sqlalchemy import text

        events = []

        try:
            async with get_db_session() as session:
                # Query events table for scheduled event markers
                query = text("""
                    SELECT
                        event_time,
                        event_type,
                        metadata->>'event_name' as event_name,
                        metadata->>'event_category' as category
                    FROM events
                    WHERE event_type IN ('SCHEDULED_EVENT', 'EARNINGS', 'ELECTION', 'ANNOUNCEMENT')
                    AND event_time BETWEEN :start AND :end
                    ORDER BY event_time
                """)

                result = await session.execute(
                    query,
                    {"start": start_time, "end": end_time},
                )
                rows = result.fetchall()

                for row in rows:
                    category = row.category or row.event_type.lower()
                    event_config = self.SCHEDULED_EVENT_TYPES.get(
                        category,
                        {"description": "Scheduled event", "window_hours": 4},
                    )

                    events.append(HardNegativeEvent(
                        event_type="scheduled_event",
                        event_time=row.event_time,
                        event_window_hours=event_config["window_hours"],
                        source=category,
                        description=row.event_name or event_config["description"],
                        confidence=0.95,
                    ))

        except Exception as e:
            logger.warning(f"Failed to query scheduled events: {e}")

        # Also check known fixed scheduled events
        events.extend(self._check_fixed_schedule(start_time, end_time))

        return events

    def _check_fixed_schedule(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[HardNegativeEvent]:
        """Check against fixed scheduled events (elections, etc.)."""
        events = []

        # US Federal Elections - first Tuesday after first Monday in November
        for year in range(start_time.year, end_time.year + 1):
            # Find first Monday in November
            nov_1 = datetime(year, 11, 1)
            days_until_monday = (7 - nov_1.weekday()) % 7
            if nov_1.weekday() == 0:
                days_until_monday = 7
            first_monday = nov_1 + timedelta(days=days_until_monday)
            election_day = first_monday + timedelta(days=1)  # Tuesday

            if start_time.date() <= election_day.date() <= end_time.date():
                events.append(HardNegativeEvent(
                    event_type="scheduled_event",
                    event_time=election_day,
                    event_window_hours=24,
                    source="election",
                    description=f"US Federal Election Day {year}",
                    confidence=1.0,
                ))

        return events


class HighVolumeFilter(HardNegativeFilter):
    """Filters based on unusually high overall news volume.

    If the total news volume is >2σ above baseline, synchronized
    timing is likely due to a major news event affecting everyone.
    """

    def __init__(
        self,
        sigma_threshold: float = 2.0,
        baseline_days: int = 30,
    ):
        """Initialize the filter.

        Args:
            sigma_threshold: Standard deviations above mean for "high volume"
            baseline_days: Days to use for calculating baseline
        """
        self.sigma_threshold = sigma_threshold
        self.baseline_days = baseline_days

    async def check_events(
        self,
        timestamps: list[datetime],
    ) -> list[HardNegativeEvent]:
        """Check if timestamps fall on high-volume days."""
        if not timestamps:
            return []

        # Group timestamps by day
        days = set(ts.date() for ts in timestamps)

        # Get baseline statistics
        baseline_stats = await self._get_baseline_stats(min(timestamps))

        if baseline_stats is None:
            return []

        mean_daily = baseline_stats["mean"]
        std_daily = baseline_stats["std"]

        if std_daily == 0:
            return []

        # Check each day
        events = []
        for day in days:
            day_count = sum(1 for ts in timestamps if ts.date() == day)

            z_score = (day_count - mean_daily) / std_daily

            if z_score > self.sigma_threshold:
                events.append(HardNegativeEvent(
                    event_type="high_volume_day",
                    event_time=datetime.combine(day, datetime.min.time()),
                    event_window_hours=24,
                    source="volume_analysis",
                    description=(
                        f"High volume day: {day_count} events "
                        f"({z_score:.1f}σ above baseline)"
                    ),
                    confidence=min(0.95, 0.5 + z_score * 0.1),
                ))

        return events

    async def _get_baseline_stats(
        self,
        reference_time: datetime,
    ) -> dict[str, float] | None:
        """Get baseline daily event statistics."""
        from ..db import get_db_session
        from sqlalchemy import text
        import numpy as np

        try:
            async with get_db_session() as session:
                start_date = reference_time - timedelta(days=self.baseline_days)

                query = text("""
                    SELECT DATE(event_time) as day, COUNT(*) as count
                    FROM events
                    WHERE event_time BETWEEN :start AND :end
                    GROUP BY DATE(event_time)
                """)

                result = await session.execute(
                    query,
                    {"start": start_date, "end": reference_time},
                )
                rows = result.fetchall()

                if not rows:
                    return None

                counts = [row.count for row in rows]

                return {
                    "mean": float(np.mean(counts)),
                    "std": float(np.std(counts)),
                    "days": len(counts),
                }

        except Exception as e:
            logger.warning(f"Failed to get baseline stats: {e}")
            return None


class HardNegativeFilterChain:
    """Chains multiple hard negative filters together."""

    def __init__(
        self,
        filters: list[HardNegativeFilter] | None = None,
    ):
        """Initialize with list of filters.

        If no filters provided, uses default set:
        - Breaking news filter (1h window)
        - Scheduled event filter
        - High volume filter (2σ threshold)
        """
        if filters is None:
            self.filters = [
                BreakingNewsFilter(window_hours=1),
                ScheduledEventFilter(),
                HighVolumeFilter(sigma_threshold=2.0),
            ]
        else:
            self.filters = filters

    async def check_all(
        self,
        timestamps: list[datetime],
    ) -> list[HardNegativeEvent]:
        """Run all filters and return combined results."""
        all_events = []

        for filter_instance in self.filters:
            try:
                events = await filter_instance.check_events(timestamps)
                all_events.extend(events)
            except Exception as e:
                logger.warning(
                    f"Filter {type(filter_instance).__name__} failed: {e}"
                )

        # Deduplicate by event time and type
        seen = set()
        unique_events = []
        for event in all_events:
            key = (event.event_type, event.event_time.isoformat())
            if key not in seen:
                seen.add(key)
                unique_events.append(event)

        return unique_events


async def filter_hard_negatives(
    events: list["TimingEvent"],
) -> list["TimingEvent"]:
    """Filter out events that fall within hard negative windows.

    Args:
        events: List of TimingEvent objects

    Returns:
        Filtered list with hard negative events removed
    """
    from .temporal import TimingEvent

    if not events:
        return events

    timestamps = [e.timestamp for e in events]

    # Run hard negative detection
    filter_chain = HardNegativeFilterChain()
    hard_negatives = await filter_chain.check_all(timestamps)

    if not hard_negatives:
        return events

    # Build set of time ranges to exclude
    exclude_ranges = []
    for hn in hard_negatives:
        start = hn.event_time - timedelta(hours=hn.event_window_hours / 2)
        end = hn.event_time + timedelta(hours=hn.event_window_hours / 2)
        exclude_ranges.append((start, end))

    # Filter events
    filtered = []
    for event in events:
        excluded = False
        for start, end in exclude_ranges:
            if start <= event.timestamp <= end:
                excluded = True
                break
        if not excluded:
            filtered.append(event)

    logger.info(
        f"Hard negative filtering: {len(events)} -> {len(filtered)} events "
        f"({len(hard_negatives)} hard negatives detected)"
    )

    return filtered


async def check_hard_negatives(
    timestamps: list[datetime],
) -> list[HardNegativeEvent]:
    """Check timestamps for hard negative explanations.

    Args:
        timestamps: List of timestamps to check

    Returns:
        List of HardNegativeEvent objects explaining timing
    """
    filter_chain = HardNegativeFilterChain()
    return await filter_chain.check_all(timestamps)
