"""Entry point adapters for the Case Intake System.

Each adapter normalizes a specific entry point type into the common
case format: evidence record + initial leads.

Available adapters:
- MetaAdAdapter: Meta Ad Library sponsor/page lookup
- CorporationAdapter: Corporation name search across registries
- URLAdapter: Fetch and extract entities from web pages
- TextAdapter: Extract entities from pasted text
"""

from .base import BaseEntryPointAdapter, SeedEntity, ValidationResult
from .meta_ads import MetaAdAdapter
from .corporation import CorporationAdapter
from .url import URLAdapter
from .text import TextAdapter

__all__ = [
    "BaseEntryPointAdapter",
    "SeedEntity",
    "ValidationResult",
    "MetaAdAdapter",
    "CorporationAdapter",
    "URLAdapter",
    "TextAdapter",
]


def get_adapter(entry_point_type: str) -> BaseEntryPointAdapter:
    """Get the appropriate adapter for an entry point type.

    Args:
        entry_point_type: The entry point type (meta_ad, corporation, url, text)

    Returns:
        The appropriate adapter instance

    Raises:
        ValueError: If the entry point type is not supported
    """
    adapters = {
        "meta_ad": MetaAdAdapter,
        "corporation": CorporationAdapter,
        "url": URLAdapter,
        "text": TextAdapter,
    }

    if entry_point_type not in adapters:
        raise ValueError(f"Unsupported entry point type: {entry_point_type}")

    return adapters[entry_point_type]()
