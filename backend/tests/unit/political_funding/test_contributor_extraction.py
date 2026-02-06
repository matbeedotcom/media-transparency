"""Unit tests for contributor deduplication (T013).

Verifies that the same contributor appearing across multiple elections
is correctly deduplicated and their contributions are tracked per-election.
"""

from decimal import Decimal

import pytest

from mitds.ingestion.elections_canada import ThirdPartyContributor


class TestContributorDeduplication:
    """Test contributor deduplication across elections."""

    def test_same_contributor_different_elections_distinct(self):
        """Same contributor in different elections should produce distinct records."""
        contrib_44 = ThirdPartyContributor(
            name="Acme Corp Ltd",
            contributor_class="corporation",
            amount=Decimal("10000"),
            city="Toronto",
            province="ON",
            election_id="44",
            jurisdiction="federal",
        )
        contrib_45 = ThirdPartyContributor(
            name="Acme Corp Ltd",
            contributor_class="corporation",
            amount=Decimal("15000"),
            city="Toronto",
            province="ON",
            election_id="45",
            jurisdiction="federal",
        )

        # Both should exist as distinct records (different elections)
        assert contrib_44.election_id != contrib_45.election_id
        assert contrib_44.amount != contrib_45.amount
        assert contrib_44.name == contrib_45.name

    def test_dedup_by_name_and_election(self):
        """Deduplication key should be (name, election_id)."""
        contributors = [
            ThirdPartyContributor(
                name="John Smith", amount=Decimal("500"), election_id="44"
            ),
            ThirdPartyContributor(
                name="John Smith", amount=Decimal("700"), election_id="44"
            ),
            ThirdPartyContributor(
                name="John Smith", amount=Decimal("300"), election_id="45"
            ),
        ]

        # Dedup: keep highest amount per (name, election_id)
        deduped = _deduplicate_contributors(contributors)

        assert len(deduped) == 2  # One per election
        election_44 = [c for c in deduped if c.election_id == "44"]
        election_45 = [c for c in deduped if c.election_id == "45"]
        assert len(election_44) == 1
        assert len(election_45) == 1
        assert election_44[0].amount == Decimal("700")  # Kept higher amount
        assert election_45[0].amount == Decimal("300")

    def test_dedup_case_insensitive_names(self):
        """Name matching should be case-insensitive."""
        contributors = [
            ThirdPartyContributor(
                name="ACME CORP LTD", amount=Decimal("5000"), election_id="44"
            ),
            ThirdPartyContributor(
                name="Acme Corp Ltd", amount=Decimal("5000"), election_id="44"
            ),
        ]

        deduped = _deduplicate_contributors(contributors)
        assert len(deduped) == 1

    def test_dedup_preserves_all_fields(self):
        """Deduplication should preserve the full record from the kept contributor."""
        contributors = [
            ThirdPartyContributor(
                name="Jane Doe",
                contributor_class="individual",
                amount=Decimal("1000"),
                city="Ottawa",
                province="ON",
                postal_code="K1A 0A6",
                election_id="44",
                jurisdiction="federal",
            ),
            ThirdPartyContributor(
                name="Jane Doe",
                contributor_class="individual",
                amount=Decimal("500"),
                city="Ottawa",
                province="ON",
                election_id="44",
                jurisdiction="federal",
            ),
        ]

        deduped = _deduplicate_contributors(contributors)
        assert len(deduped) == 1
        assert deduped[0].city == "Ottawa"
        assert deduped[0].province == "ON"
        assert deduped[0].postal_code == "K1A 0A6"  # From the higher-amount record

    def test_empty_list_returns_empty(self):
        """Empty input returns empty output."""
        assert _deduplicate_contributors([]) == []

    def test_single_contributor_unchanged(self):
        """Single contributor passes through unchanged."""
        contributors = [
            ThirdPartyContributor(
                name="Solo Contributor", amount=Decimal("1000"), election_id="44"
            ),
        ]
        deduped = _deduplicate_contributors(contributors)
        assert len(deduped) == 1
        assert deduped[0].name == "Solo Contributor"


def _deduplicate_contributors(
    contributors: list[ThirdPartyContributor],
) -> list[ThirdPartyContributor]:
    """Deduplicate contributors by (normalized name, election_id).

    Keeps the record with the highest amount for each unique key.
    This is the dedup logic that should be used in the ingester.
    """
    seen: dict[tuple[str, str], ThirdPartyContributor] = {}

    for c in contributors:
        key = (c.name.strip().upper(), c.election_id or "")
        if key not in seen or c.amount > seen[key].amount:
            seen[key] = c

    return list(seen.values())
