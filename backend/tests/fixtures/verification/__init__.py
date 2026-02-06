"""Verification fixtures for real-data testing.

This package contains reference JSON fixtures with manually verified data
from public sources. These are used by the verification test suites
(`mitds verify <source>`) to validate ingestion accuracy.

Each file contains:
- Entity names and expected attributes
- Expected relationships and their properties
- Source-specific expected output

No phase is considered complete until its verification suite passes.
"""
