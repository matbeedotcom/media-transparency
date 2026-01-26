"""Temporal coordination detection for MITDS.

Detects patterns of coordinated timing in publication/ad delivery:
1. Burst detection (Kleinberg's automaton model)
2. Lead-lag correlation analysis (Granger causality)
3. Synchronization scoring (Jensen-Shannon divergence)
"""

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import numpy as np
from pydantic import BaseModel, Field

from ..logging import get_context_logger

logger = get_context_logger(__name__)


class TimingEvent(BaseModel):
    """A timestamped event for temporal analysis."""

    entity_id: str
    timestamp: datetime
    event_type: str = "publication"
    metadata: dict[str, Any] = Field(default_factory=dict)


class BurstDetectionResult(BaseModel):
    """Result from Kleinberg burst detection."""

    entity_id: str
    bursts: list[dict[str, Any]] = Field(default_factory=list)
    total_events: int = 0
    burst_count: int = 0
    avg_events_per_day: float = 0.0


class LeadLagResult(BaseModel):
    """Result from lead-lag correlation analysis."""

    leader_entity_id: str
    follower_entity_id: str
    lag_minutes: int
    correlation: float
    p_value: float
    sample_size: int
    is_significant: bool = False


class SynchronizationResult(BaseModel):
    """Result from synchronization scoring."""

    entity_ids: list[str]
    sync_score: float = Field(ge=0.0, le=1.0)
    js_divergence: float
    overlap_ratio: float
    time_window_hours: int
    confidence: float = Field(ge=0.0, le=1.0)


class TemporalCoordinationResult(BaseModel):
    """Combined result of all temporal coordination detection."""

    analysis_id: str
    analyzed_at: datetime
    time_range_start: datetime
    time_range_end: datetime
    entity_count: int
    event_count: int

    # Component results
    bursts: list[BurstDetectionResult] = Field(default_factory=list)
    lead_lag_pairs: list[LeadLagResult] = Field(default_factory=list)
    synchronized_groups: list[SynchronizationResult] = Field(default_factory=list)

    # Overall assessment
    coordination_score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    is_coordinated: bool = False
    explanation: str = ""


@dataclass
class KleinbergBurstState:
    """State for Kleinberg automaton burst detection."""

    level: int = 0
    cost: float = 0.0
    path: list = field(default_factory=list)


class BurstDetector:
    """Implements Kleinberg's automaton model for burst detection.

    Based on: Kleinberg, J. "Bursty and Hierarchical Structure in Streams"
    (KDD 2002). Uses a probabilistic automaton with multiple states
    representing different activity levels.

    Parameters:
        s: Scaling parameter for state transition (default: 2.0)
        gamma: Cost for state transitions (default: 1.0)
        min_burst_events: Minimum events to constitute a burst (default: 3)
    """

    def __init__(
        self,
        s: float = 2.0,
        gamma: float = 1.0,
        min_burst_events: int = 3,
    ):
        self.s = s
        self.gamma = gamma
        self.min_burst_events = min_burst_events

    def detect_bursts(
        self,
        events: list[TimingEvent],
        entity_id: str | None = None,
    ) -> BurstDetectionResult:
        """Detect bursts in a sequence of events.

        Args:
            events: List of timing events (will be filtered by entity_id if provided)
            entity_id: Optional entity to filter events by

        Returns:
            BurstDetectionResult with identified burst periods
        """
        if entity_id:
            events = [e for e in events if e.entity_id == entity_id]

        if len(events) < self.min_burst_events:
            return BurstDetectionResult(
                entity_id=entity_id or "all",
                total_events=len(events),
            )

        # Sort events by timestamp
        events = sorted(events, key=lambda e: e.timestamp)

        # Convert to inter-arrival times (in minutes)
        gaps = []
        for i in range(1, len(events)):
            delta = (events[i].timestamp - events[i - 1].timestamp).total_seconds() / 60
            gaps.append(max(delta, 0.1))  # Avoid zero gaps

        if not gaps:
            return BurstDetectionResult(
                entity_id=entity_id or "all",
                total_events=len(events),
            )

        # Calculate base rate (expected gap between events)
        total_time = (events[-1].timestamp - events[0].timestamp).total_seconds() / 60
        n = len(gaps)
        base_rate = total_time / n if n > 0 else 1.0

        # Calculate number of states needed
        max_rate = max(gaps) if gaps else 1.0
        k = max(2, int(math.ceil(1 + math.log(max_rate / base_rate, self.s))) + 1)

        # Run Viterbi algorithm to find optimal state sequence
        states = self._viterbi(gaps, base_rate, k)

        # Extract burst periods (states > 0)
        bursts = self._extract_bursts(events, states)

        # Calculate statistics
        time_range = (events[-1].timestamp - events[0].timestamp).days or 1

        return BurstDetectionResult(
            entity_id=entity_id or "all",
            bursts=bursts,
            total_events=len(events),
            burst_count=len(bursts),
            avg_events_per_day=len(events) / time_range,
        )

    def _viterbi(
        self,
        gaps: list[float],
        base_rate: float,
        k: int,
    ) -> list[int]:
        """Viterbi algorithm for finding optimal state sequence."""
        n = len(gaps)

        # Initialize state costs
        # cost[i][j] = min cost to reach state j at position i
        cost = [[float('inf')] * k for _ in range(n + 1)]
        parent = [[0] * k for _ in range(n + 1)]

        # Initial state is 0 (base rate)
        cost[0][0] = 0

        # State rates: rate[j] = base_rate * s^j
        rates = [base_rate * (self.s ** j) for j in range(k)]

        for i in range(n):
            gap = gaps[i]

            for j in range(k):
                if cost[i][j] == float('inf'):
                    continue

                # Calculate emission cost (negative log likelihood)
                # Using exponential distribution: P(gap|rate) = rate * exp(-rate * gap)
                for j_next in range(k):
                    rate = rates[j_next]

                    # Emission cost
                    if rate > 0 and gap > 0:
                        emit_cost = rate * gap - math.log(rate)
                    else:
                        emit_cost = float('inf')

                    # Transition cost
                    trans_cost = self.gamma * max(0, j_next - j) if j_next != j else 0

                    total_cost = cost[i][j] + emit_cost + trans_cost

                    if total_cost < cost[i + 1][j_next]:
                        cost[i + 1][j_next] = total_cost
                        parent[i + 1][j_next] = j

        # Backtrack to find optimal state sequence
        states = [0] * n

        # Find minimum cost final state
        min_cost = float('inf')
        last_state = 0
        for j in range(k):
            if cost[n][j] < min_cost:
                min_cost = cost[n][j]
                last_state = j

        # Backtrack
        current_state = last_state
        for i in range(n - 1, -1, -1):
            states[i] = current_state
            current_state = parent[i + 1][current_state]

        return states

    def _extract_bursts(
        self,
        events: list[TimingEvent],
        states: list[int],
    ) -> list[dict[str, Any]]:
        """Extract burst periods from state sequence."""
        bursts = []

        burst_start = None
        burst_level = 0
        burst_events = []

        for i, state in enumerate(states):
            if state > 0:
                if burst_start is None:
                    burst_start = i
                    burst_level = state
                    burst_events = [events[i]]
                else:
                    burst_level = max(burst_level, state)
                    burst_events.append(events[i])
            else:
                if burst_start is not None and len(burst_events) >= self.min_burst_events:
                    bursts.append({
                        "start_time": events[burst_start].timestamp.isoformat(),
                        "end_time": events[i - 1].timestamp.isoformat() if i > 0 else events[burst_start].timestamp.isoformat(),
                        "level": burst_level,
                        "event_count": len(burst_events),
                        "duration_hours": (
                            (events[i - 1].timestamp - events[burst_start].timestamp).total_seconds() / 3600
                            if i > 0 else 0
                        ),
                    })
                burst_start = None
                burst_events = []

        # Handle burst at end
        if burst_start is not None and len(burst_events) >= self.min_burst_events:
            bursts.append({
                "start_time": events[burst_start].timestamp.isoformat(),
                "end_time": events[-1].timestamp.isoformat(),
                "level": burst_level,
                "event_count": len(burst_events),
                "duration_hours": (
                    (events[-1].timestamp - events[burst_start].timestamp).total_seconds() / 3600
                ),
            })

        return bursts


class LeadLagAnalyzer:
    """Analyzes lead-lag relationships between entities.

    Uses cross-correlation to identify if one entity consistently
    publishes/acts before another, suggesting coordination.
    """

    def __init__(
        self,
        max_lag_minutes: int = 1440,  # 24 hours
        lag_step_minutes: int = 30,
        min_samples: int = 10,
        significance_threshold: float = 0.05,
    ):
        self.max_lag_minutes = max_lag_minutes
        self.lag_step_minutes = lag_step_minutes
        self.min_samples = min_samples
        self.significance_threshold = significance_threshold

    def analyze_pair(
        self,
        events: list[TimingEvent],
        entity_a: str,
        entity_b: str,
    ) -> LeadLagResult | None:
        """Analyze lead-lag relationship between two entities.

        Args:
            events: All timing events
            entity_a: First entity ID
            entity_b: Second entity ID

        Returns:
            LeadLagResult if significant relationship found, None otherwise
        """
        events_a = [e for e in events if e.entity_id == entity_a]
        events_b = [e for e in events if e.entity_id == entity_b]

        if len(events_a) < self.min_samples or len(events_b) < self.min_samples:
            return None

        # Convert to time series (counts per hour)
        all_events = events_a + events_b
        start_time = min(e.timestamp for e in all_events)
        end_time = max(e.timestamp for e in all_events)

        total_hours = int((end_time - start_time).total_seconds() / 3600) + 1
        if total_hours < 2:
            return None

        # Create hourly count arrays
        series_a = np.zeros(total_hours)
        series_b = np.zeros(total_hours)

        for e in events_a:
            hour_idx = int((e.timestamp - start_time).total_seconds() / 3600)
            if 0 <= hour_idx < total_hours:
                series_a[hour_idx] += 1

        for e in events_b:
            hour_idx = int((e.timestamp - start_time).total_seconds() / 3600)
            if 0 <= hour_idx < total_hours:
                series_b[hour_idx] += 1

        # Calculate cross-correlation at different lags
        best_corr = 0.0
        best_lag = 0

        max_lag_hours = self.max_lag_minutes // 60

        for lag in range(-max_lag_hours, max_lag_hours + 1):
            if lag < 0:
                corr = self._correlation(series_a[-lag:], series_b[:lag])
            elif lag > 0:
                corr = self._correlation(series_a[:-lag], series_b[lag:])
            else:
                corr = self._correlation(series_a, series_b)

            if abs(corr) > abs(best_corr):
                best_corr = corr
                best_lag = lag

        # Calculate p-value using permutation test
        p_value = self._permutation_test(series_a, series_b, best_corr)

        # Determine leader and follower
        if best_lag > 0:
            # A leads B
            leader = entity_a
            follower = entity_b
            lag_minutes = best_lag * 60
        else:
            # B leads A
            leader = entity_b
            follower = entity_a
            lag_minutes = abs(best_lag) * 60

        is_significant = (
            p_value < self.significance_threshold and
            abs(best_corr) > 0.3  # Minimum correlation threshold
        )

        return LeadLagResult(
            leader_entity_id=leader,
            follower_entity_id=follower,
            lag_minutes=lag_minutes,
            correlation=float(best_corr),
            p_value=float(p_value),
            sample_size=min(len(events_a), len(events_b)),
            is_significant=is_significant,
        )

    def _correlation(self, a: np.ndarray, b: np.ndarray) -> float:
        """Calculate Pearson correlation between two arrays."""
        if len(a) == 0 or len(b) == 0:
            return 0.0

        # Ensure same length
        min_len = min(len(a), len(b))
        a = a[:min_len]
        b = b[:min_len]

        # Handle constant arrays
        if np.std(a) == 0 or np.std(b) == 0:
            return 0.0

        return float(np.corrcoef(a, b)[0, 1])

    def _permutation_test(
        self,
        series_a: np.ndarray,
        series_b: np.ndarray,
        observed_corr: float,
        n_permutations: int = 1000,
    ) -> float:
        """Estimate p-value using permutation test."""
        count_extreme = 0

        for _ in range(n_permutations):
            # Shuffle one series
            shuffled_b = np.random.permutation(series_b)
            perm_corr = self._correlation(series_a, shuffled_b)

            if abs(perm_corr) >= abs(observed_corr):
                count_extreme += 1

        return (count_extreme + 1) / (n_permutations + 1)


class SynchronizationScorer:
    """Scores synchronization between multiple entities.

    Uses Jensen-Shannon divergence to compare timing distributions.
    Lower divergence = higher synchronization.
    """

    def __init__(
        self,
        time_window_hours: int = 24,
        min_events_per_entity: int = 5,
    ):
        self.time_window_hours = time_window_hours
        self.min_events_per_entity = min_events_per_entity

    def score_group(
        self,
        events: list[TimingEvent],
        entity_ids: list[str],
    ) -> SynchronizationResult | None:
        """Score synchronization for a group of entities.

        Args:
            events: All timing events
            entity_ids: Entity IDs to analyze

        Returns:
            SynchronizationResult with sync score
        """
        if len(entity_ids) < 2:
            return None

        # Filter events for specified entities
        entity_events = defaultdict(list)
        for event in events:
            if event.entity_id in entity_ids:
                entity_events[event.entity_id].append(event)

        # Check minimum events
        valid_entities = [
            eid for eid, evts in entity_events.items()
            if len(evts) >= self.min_events_per_entity
        ]

        if len(valid_entities) < 2:
            return None

        # Build timing distributions (hour of day)
        distributions = {}
        for entity_id in valid_entities:
            dist = np.zeros(24)  # 24 hours
            for event in entity_events[entity_id]:
                hour = event.timestamp.hour
                dist[hour] += 1

            # Normalize to probability distribution
            total = dist.sum()
            if total > 0:
                dist = dist / total
            distributions[entity_id] = dist

        # Calculate pairwise JS divergence
        js_divergences = []
        for i, eid_a in enumerate(valid_entities):
            for eid_b in valid_entities[i + 1:]:
                js = self._jensen_shannon_divergence(
                    distributions[eid_a],
                    distributions[eid_b],
                )
                js_divergences.append(js)

        avg_js = np.mean(js_divergences) if js_divergences else 1.0

        # Calculate overlap ratio
        overlap = self._calculate_overlap(entity_events, valid_entities)

        # Sync score: 1 - normalized JS divergence
        # JS divergence is in [0, ln(2)] for distributions, normalize to [0, 1]
        sync_score = max(0.0, 1.0 - avg_js / math.log(2))

        # Confidence based on sample size
        total_events = sum(len(entity_events[eid]) for eid in valid_entities)
        confidence = min(1.0, total_events / (len(valid_entities) * 50))

        return SynchronizationResult(
            entity_ids=valid_entities,
            sync_score=float(sync_score),
            js_divergence=float(avg_js),
            overlap_ratio=float(overlap),
            time_window_hours=self.time_window_hours,
            confidence=float(confidence),
        )

    def _jensen_shannon_divergence(self, p: np.ndarray, q: np.ndarray) -> float:
        """Calculate Jensen-Shannon divergence between two distributions."""
        # Add small epsilon to avoid log(0)
        epsilon = 1e-10
        p = p + epsilon
        q = q + epsilon

        # Normalize
        p = p / p.sum()
        q = q / q.sum()

        # Midpoint distribution
        m = (p + q) / 2

        # KL divergences
        kl_pm = np.sum(p * np.log(p / m))
        kl_qm = np.sum(q * np.log(q / m))

        return (kl_pm + kl_qm) / 2

    def _calculate_overlap(
        self,
        entity_events: dict[str, list[TimingEvent]],
        entity_ids: list[str],
    ) -> float:
        """Calculate temporal overlap ratio between entities."""
        if len(entity_ids) < 2:
            return 0.0

        # Bin events by time window
        window_seconds = self.time_window_hours * 3600

        time_bins = defaultdict(set)
        for entity_id in entity_ids:
            for event in entity_events[entity_id]:
                # Round timestamp to window
                ts = event.timestamp.timestamp()
                bin_key = int(ts // window_seconds)
                time_bins[bin_key].add(entity_id)

        # Count bins with multiple entities
        multi_entity_bins = sum(1 for entities in time_bins.values() if len(entities) > 1)
        total_bins = len(time_bins)

        return multi_entity_bins / total_bins if total_bins > 0 else 0.0


class TemporalCoordinationDetector:
    """Main coordinator for temporal coordination detection.

    Combines burst detection, lead-lag analysis, and synchronization
    scoring to identify potential coordinated behavior.
    """

    def __init__(
        self,
        burst_detector: BurstDetector | None = None,
        lead_lag_analyzer: LeadLagAnalyzer | None = None,
        sync_scorer: SynchronizationScorer | None = None,
    ):
        self.burst_detector = burst_detector or BurstDetector()
        self.lead_lag_analyzer = lead_lag_analyzer or LeadLagAnalyzer()
        self.sync_scorer = sync_scorer or SynchronizationScorer()

    async def detect_coordination(
        self,
        events: list[TimingEvent],
        entity_ids: list[str] | None = None,
        exclude_hard_negatives: bool = True,
    ) -> TemporalCoordinationResult:
        """Run full temporal coordination detection.

        Args:
            events: List of timing events to analyze
            entity_ids: Optional list of entity IDs to focus on
            exclude_hard_negatives: Whether to apply hard negative filtering

        Returns:
            TemporalCoordinationResult with all analysis
        """
        from uuid import uuid4

        if not events:
            return TemporalCoordinationResult(
                analysis_id=str(uuid4()),
                analyzed_at=datetime.utcnow(),
                time_range_start=datetime.utcnow(),
                time_range_end=datetime.utcnow(),
                entity_count=0,
                event_count=0,
                explanation="No events to analyze",
            )

        # Filter to specified entities if provided
        if entity_ids:
            events = [e for e in events if e.entity_id in entity_ids]

        # Get unique entities
        unique_entities = list(set(e.entity_id for e in events))

        # Calculate time range
        timestamps = [e.timestamp for e in events]
        time_start = min(timestamps)
        time_end = max(timestamps)

        # Apply hard negative filtering if enabled
        if exclude_hard_negatives:
            from .hardneg import filter_hard_negatives
            events = await filter_hard_negatives(events)

        # Run burst detection for each entity
        bursts = []
        for entity_id in unique_entities:
            burst_result = self.burst_detector.detect_bursts(events, entity_id)
            if burst_result.burst_count > 0:
                bursts.append(burst_result)

        # Run pairwise lead-lag analysis
        lead_lag_pairs = []
        for i, entity_a in enumerate(unique_entities):
            for entity_b in unique_entities[i + 1:]:
                result = self.lead_lag_analyzer.analyze_pair(events, entity_a, entity_b)
                if result and result.is_significant:
                    lead_lag_pairs.append(result)

        # Run synchronization scoring
        sync_result = self.sync_scorer.score_group(events, unique_entities)
        synchronized_groups = [sync_result] if sync_result else []

        # Calculate overall coordination score
        coordination_score = self._calculate_coordination_score(
            bursts, lead_lag_pairs, synchronized_groups
        )

        # Determine if coordinated
        is_coordinated = coordination_score > 0.5

        # Generate explanation
        explanation = self._generate_explanation(
            bursts, lead_lag_pairs, synchronized_groups, coordination_score
        )

        # Calculate confidence
        confidence = min(1.0, len(events) / 100) * min(1.0, len(unique_entities) / 5)

        return TemporalCoordinationResult(
            analysis_id=str(uuid4()),
            analyzed_at=datetime.utcnow(),
            time_range_start=time_start,
            time_range_end=time_end,
            entity_count=len(unique_entities),
            event_count=len(events),
            bursts=bursts,
            lead_lag_pairs=lead_lag_pairs,
            synchronized_groups=synchronized_groups,
            coordination_score=coordination_score,
            confidence=confidence,
            is_coordinated=is_coordinated,
            explanation=explanation,
        )

    def _calculate_coordination_score(
        self,
        bursts: list[BurstDetectionResult],
        lead_lag_pairs: list[LeadLagResult],
        sync_groups: list[SynchronizationResult],
    ) -> float:
        """Calculate composite coordination score from components."""
        scores = []

        # Burst score: ratio of entities with bursts
        if bursts:
            burst_ratio = len([b for b in bursts if b.burst_count > 0]) / len(bursts)
            scores.append(burst_ratio * 0.3)

        # Lead-lag score: presence of significant lead-lag relationships
        if lead_lag_pairs:
            significant_pairs = len([p for p in lead_lag_pairs if p.is_significant])
            pair_score = min(1.0, significant_pairs / 3)
            scores.append(pair_score * 0.3)

        # Synchronization score
        if sync_groups:
            avg_sync = sum(g.sync_score for g in sync_groups) / len(sync_groups)
            scores.append(avg_sync * 0.4)

        return sum(scores) if scores else 0.0

    def _generate_explanation(
        self,
        bursts: list[BurstDetectionResult],
        lead_lag_pairs: list[LeadLagResult],
        sync_groups: list[SynchronizationResult],
        score: float,
    ) -> str:
        """Generate human-readable explanation of findings."""
        parts = []

        # Describe bursts
        entities_with_bursts = [b.entity_id for b in bursts if b.burst_count > 0]
        if entities_with_bursts:
            parts.append(
                f"Detected publication bursts in {len(entities_with_bursts)} entities."
            )

        # Describe lead-lag
        if lead_lag_pairs:
            significant = [p for p in lead_lag_pairs if p.is_significant]
            if significant:
                top_pair = max(significant, key=lambda p: abs(p.correlation))
                parts.append(
                    f"Found lead-lag relationship: {top_pair.leader_entity_id} "
                    f"leads {top_pair.follower_entity_id} by ~{top_pair.lag_minutes} minutes "
                    f"(correlation: {top_pair.correlation:.2f})."
                )

        # Describe synchronization
        if sync_groups:
            top_sync = max(sync_groups, key=lambda g: g.sync_score)
            if top_sync.sync_score > 0.5:
                parts.append(
                    f"High timing synchronization detected "
                    f"(sync score: {top_sync.sync_score:.2f})."
                )

        if not parts:
            parts.append("No significant temporal coordination patterns detected.")

        # Add overall assessment
        if score > 0.7:
            parts.append("Overall: Strong indicators of coordinated timing.")
        elif score > 0.5:
            parts.append("Overall: Moderate indicators of coordinated timing.")
        elif score > 0.3:
            parts.append("Overall: Weak indicators of possible coordination.")
        else:
            parts.append("Overall: Timing patterns appear independent.")

        return " ".join(parts)


# Convenience functions
async def detect_temporal_coordination(
    events: list[TimingEvent],
    entity_ids: list[str] | None = None,
    exclude_hard_negatives: bool = True,
) -> TemporalCoordinationResult:
    """Detect temporal coordination patterns in events.

    Args:
        events: List of timing events
        entity_ids: Optional specific entities to analyze
        exclude_hard_negatives: Whether to filter out legitimate coordinations

    Returns:
        TemporalCoordinationResult with analysis
    """
    detector = TemporalCoordinationDetector()
    return await detector.detect_coordination(
        events=events,
        entity_ids=entity_ids,
        exclude_hard_negatives=exclude_hard_negatives,
    )


def detect_bursts(
    events: list[TimingEvent],
    entity_id: str | None = None,
) -> BurstDetectionResult:
    """Detect publication bursts for an entity.

    Args:
        events: List of timing events
        entity_id: Optional entity to filter by

    Returns:
        BurstDetectionResult with identified bursts
    """
    detector = BurstDetector()
    return detector.detect_bursts(events, entity_id)
