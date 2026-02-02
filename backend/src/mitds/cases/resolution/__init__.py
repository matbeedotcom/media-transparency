"""Entity resolution for the Case Intake System.

Handles sponsor-to-organization resolution with confidence-based routing:
- >= 0.9: Auto-merge
- 0.7-0.9: Queue for human review
- < 0.7: Discard

Components:
- SponsorResolver: Match Meta Ad sponsors to known organizations
"""

__all__: list[str] = []
