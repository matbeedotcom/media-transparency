"""Pagination utilities for API endpoints.

Provides standardized pagination across all list endpoints.
"""

from typing import Any, Generic, TypeVar

from fastapi import Query
from pydantic import BaseModel, Field

T = TypeVar("T")


class PaginationParams:
    """Common pagination parameters for dependency injection."""

    def __init__(
        self,
        limit: int = Query(20, ge=1, le=100, description="Maximum results to return"),
        offset: int = Query(0, ge=0, description="Number of results to skip"),
    ):
        self.limit = limit
        self.offset = offset


class CursorPaginationParams:
    """Cursor-based pagination parameters for large datasets."""

    def __init__(
        self,
        cursor: str | None = Query(None, description="Pagination cursor from previous response"),
        limit: int = Query(20, ge=1, le=100, description="Maximum results to return"),
    ):
        self.cursor = cursor
        self.limit = limit


class PaginatedResult(BaseModel, Generic[T]):
    """Generic paginated result model."""

    results: list[T]
    total: int = Field(..., description="Total number of results available")
    limit: int = Field(..., description="Maximum results requested")
    offset: int = Field(..., description="Number of results skipped")
    has_more: bool = Field(..., description="Whether more results are available")

    @classmethod
    def create(
        cls,
        results: list[T],
        total: int,
        limit: int,
        offset: int,
    ) -> "PaginatedResult[T]":
        """Create a paginated result.

        Args:
            results: Results for current page
            total: Total count of all results
            limit: Page size
            offset: Current offset

        Returns:
            Paginated result
        """
        return cls(
            results=results,
            total=total,
            limit=limit,
            offset=offset,
            has_more=offset + len(results) < total,
        )


class CursorPaginatedResult(BaseModel, Generic[T]):
    """Cursor-based paginated result for large datasets."""

    results: list[T]
    next_cursor: str | None = Field(None, description="Cursor for next page")
    has_more: bool = Field(..., description="Whether more results are available")

    @classmethod
    def create(
        cls,
        results: list[T],
        next_cursor: str | None,
    ) -> "CursorPaginatedResult[T]":
        """Create a cursor-paginated result.

        Args:
            results: Results for current page
            next_cursor: Cursor for fetching next page (None if no more pages)

        Returns:
            Cursor-paginated result
        """
        return cls(
            results=results,
            next_cursor=next_cursor,
            has_more=next_cursor is not None,
        )


def paginate_list(
    items: list[T],
    total: int | None,
    params: PaginationParams,
) -> dict[str, Any]:
    """Apply pagination to a list of items.

    If items have already been sliced, total should be the original count.
    If items is the full list, it will be sliced here.

    Args:
        items: List of items (may be pre-sliced or full list)
        total: Total count (None to use len(items))
        params: Pagination parameters

    Returns:
        Dictionary with paginated response structure
    """
    if total is None:
        total = len(items)
        # Slice the items if we have the full list
        items = items[params.offset : params.offset + params.limit]

    return {
        "results": items,
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
        "has_more": params.offset + len(items) < total,
    }


def encode_cursor(value: str | int, timestamp: str | None = None) -> str:
    """Encode a cursor value for pagination.

    Args:
        value: Primary cursor value (usually last item's ID or sort key)
        timestamp: Optional timestamp for stable sorting

    Returns:
        Encoded cursor string
    """
    import base64
    import json

    cursor_data = {"v": value}
    if timestamp:
        cursor_data["t"] = timestamp

    return base64.urlsafe_b64encode(
        json.dumps(cursor_data).encode()
    ).decode()


def decode_cursor(cursor: str) -> dict[str, Any] | None:
    """Decode a cursor string.

    Args:
        cursor: Encoded cursor string

    Returns:
        Decoded cursor data or None if invalid
    """
    import base64
    import json

    try:
        decoded = base64.urlsafe_b64decode(cursor.encode())
        return json.loads(decoded)
    except (ValueError, json.JSONDecodeError):
        return None


# =========================
# SQL Pagination Helpers
# =========================


def sql_paginate(
    query: str,
    params: PaginationParams,
    order_by: str = "id",
) -> str:
    """Add pagination to a SQL query.

    Args:
        query: Base SQL query
        params: Pagination parameters
        order_by: Column to order by

    Returns:
        Paginated SQL query
    """
    return f"""
    {query}
    ORDER BY {order_by}
    LIMIT {params.limit}
    OFFSET {params.offset}
    """


def sql_count(query: str) -> str:
    """Wrap a query to get total count.

    Args:
        query: Base SQL query (should not have ORDER BY, LIMIT, OFFSET)

    Returns:
        Count query
    """
    return f"""
    SELECT COUNT(*) as total
    FROM ({query}) as count_subquery
    """


# =========================
# Cypher Pagination Helpers
# =========================


def cypher_paginate(
    query: str,
    params: PaginationParams,
    order_by: str = "n.name",
) -> str:
    """Add pagination to a Cypher query.

    Args:
        query: Base Cypher query
        params: Pagination parameters
        order_by: Property to order by

    Returns:
        Paginated Cypher query
    """
    # Only add ORDER BY if not already present
    if "ORDER BY" not in query.upper():
        query = f"{query}\nORDER BY {order_by}"

    return f"""
    {query}
    SKIP {params.offset}
    LIMIT {params.limit}
    """


def cypher_count(match_clause: str, where_clause: str = "") -> str:
    """Create a count query for Cypher.

    Args:
        match_clause: MATCH clause of the query
        where_clause: Optional WHERE clause

    Returns:
        Count query
    """
    where = f"WHERE {where_clause}" if where_clause else ""
    return f"""
    {match_clause}
    {where}
    RETURN count(*) as total
    """
