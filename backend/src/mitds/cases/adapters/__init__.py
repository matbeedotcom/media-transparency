"""Entry point adapters for the Case Intake System.

Each adapter normalizes a specific entry point type into the common
case format: evidence record + initial leads.

Available adapters:
- MetaAdAdapter: Meta Ad Library sponsor/page lookup
- CorporationAdapter: Corporation name search across registries
- URLAdapter: Fetch and extract entities from web pages
- TextAdapter: Extract entities from pasted text
"""

from .base import BaseEntryPointAdapter, ValidationResult

__all__ = [
    "BaseEntryPointAdapter",
    "ValidationResult",
]
