"""Research module for MITDS.

Provides the "follow the leads" dynamic ingestion system that
transforms MITDS into an active research platform for investigating
media influence networks.

Main components:
- ResearchSessionManager: Manages research sessions
- LeadQueueManager: Priority queue for leads
- LeadProcessor: Processes leads and orchestrates ingestion
- Lead extractors: Discover new leads from entities

Usage:
    from mitds.research import (
        get_session_manager,
        get_queue_manager,
        get_processor,
    )

    # Create a new research session
    manager = get_session_manager()
    session = await manager.create_session(
        name="PAC Investigation",
        entry_point_type="ein",
        entry_point_value="13-1837418",
    )

    # Process the session
    processor = get_processor()
    stats = await processor.process_session(session.id)
"""

from .models import (
    CreateSessionRequest,
    EntryPointType,
    IdentifierType,
    Lead,
    LeadResult,
    LeadStatus,
    LeadSummary,
    LeadType,
    QueuedLead,
    QueueStats,
    ResearchSession,
    ResearchSessionConfig,
    SessionGraph,
    SessionResponse,
    SessionStats,
    SessionStatus,
    SingleIngestionResult,
)
from .session import ResearchSessionManager, get_session_manager
from .queue import LeadQueueManager, get_queue_manager
from .processor import LeadProcessor, get_processor
from .registry import (
    INGESTER_CAPABILITIES,
    IngesterCapability,
    get_capability,
    get_ingesters_for_identifier,
    get_ingesters_for_jurisdiction,
    get_ingesters_for_lead_type,
)

__all__ = [
    # Models
    "CreateSessionRequest",
    "EntryPointType",
    "IdentifierType",
    "Lead",
    "LeadResult",
    "LeadStatus",
    "LeadSummary",
    "LeadType",
    "QueuedLead",
    "QueueStats",
    "ResearchSession",
    "ResearchSessionConfig",
    "SessionGraph",
    "SessionResponse",
    "SessionStats",
    "SessionStatus",
    "SingleIngestionResult",
    # Managers
    "ResearchSessionManager",
    "get_session_manager",
    "LeadQueueManager",
    "get_queue_manager",
    "LeadProcessor",
    "get_processor",
    # Registry
    "INGESTER_CAPABILITIES",
    "IngesterCapability",
    "get_capability",
    "get_ingesters_for_identifier",
    "get_ingesters_for_jurisdiction",
    "get_ingesters_for_lead_type",
]
