"""Minimal tool gateway for LLM research workflows.

Exposes a small, curated set of Research tools without publishing the full OpenAPI.
"""

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from mitds.research.models import (
    CreateSessionRequest,
    LeadSummary,
    QueueStats,
    SessionGraph,
    SessionResponse,
    SessionStatus,
)

router = APIRouter(prefix="/tools", tags=["tools"])


class EntitySummaryResponse(BaseModel):
    id: str
    name: str
    entity_type: str
    depth: int
    relevance_score: float


class EntitiesListResponse(BaseModel):
    entities: list[EntitySummaryResponse]
    total: int


class LeadsListResponse(BaseModel):
    leads: list[LeadSummary]
    total: int
    stats: QueueStats


class SkipLeadRequest(BaseModel):
    reason: str


class PrioritizeLeadRequest(BaseModel):
    priority: int


class SessionListResponse(BaseModel):
    sessions: list[SessionResponse]
    total: int


class ToolDefinition(BaseModel):
    name: str
    description: str
    method: str
    path: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]


class ToolListResponse(BaseModel):
    tools: list[ToolDefinition]


def _schema_for_model(model: type[BaseModel]) -> dict[str, Any]:
    return model.model_json_schema()


def _params_schema(
    properties: dict[str, Any],
    required: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": required or [],
        "additionalProperties": False,
    }


def _input_schema(
    *,
    path: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
    required: list[str] | None = None,
) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    required_fields: list[str] = []

    if path is not None:
        properties["path"] = path
        required_fields.append("path")
    if query is not None:
        properties["query"] = query
    if body is not None:
        properties["body"] = body
        required_fields.append("body")

    if required:
        required_fields.extend(required)

    return {
        "type": "object",
        "properties": properties,
        "required": required_fields,
        "additionalProperties": False,
    }


def _research_tools() -> list[ToolDefinition]:
    return [
        ToolDefinition(
            name="create_research_session",
            description="Create a new research session from an entry point.",
            method="POST",
            path="/api/v1/research/sessions",
            input_schema=_input_schema(body=_schema_for_model(CreateSessionRequest)),
            output_schema=_schema_for_model(SessionResponse),
        ),
        ToolDefinition(
            name="start_research_session",
            description="Start or resume processing of a research session.",
            method="POST",
            path="/api/v1/research/sessions/{session_id}/start",
            input_schema=_input_schema(
                path=_params_schema(
                    {
                        "session_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Research session ID",
                        }
                    },
                    required=["session_id"],
                )
            ),
            output_schema=_schema_for_model(SessionResponse),
        ),
        ToolDefinition(
            name="get_research_session",
            description="Fetch current state and stats for a session.",
            method="GET",
            path="/api/v1/research/sessions/{session_id}",
            input_schema=_input_schema(
                path=_params_schema(
                    {
                        "session_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Research session ID",
                        }
                    },
                    required=["session_id"],
                )
            ),
            output_schema=_schema_for_model(SessionResponse),
        ),
        ToolDefinition(
            name="list_research_sessions",
            description="List research sessions with optional status filter.",
            method="GET",
            path="/api/v1/research/sessions",
            input_schema=_input_schema(
                query=_params_schema(
                    {
                        "status": {
                            "type": "string",
                            "enum": [status.value for status in SessionStatus],
                            "description": "Filter by session status",
                        },
                        "limit": {"type": "integer", "default": 50, "maximum": 100},
                        "offset": {"type": "integer", "default": 0, "minimum": 0},
                    }
                )
            ),
            output_schema=_schema_for_model(SessionListResponse),
        ),
        ToolDefinition(
            name="get_session_entities",
            description="List entities discovered in a research session.",
            method="GET",
            path="/api/v1/research/sessions/{session_id}/entities",
            input_schema=_input_schema(
                path=_params_schema(
                    {
                        "session_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Research session ID",
                        }
                    },
                    required=["session_id"],
                ),
                query=_params_schema(
                    {
                        "depth": {"type": "integer"},
                        "entity_type": {"type": "string"},
                        "limit": {"type": "integer", "default": 100, "maximum": 1000},
                        "offset": {"type": "integer", "default": 0, "minimum": 0},
                    }
                ),
            ),
            output_schema=_schema_for_model(EntitiesListResponse),
        ),
        ToolDefinition(
            name="get_session_graph",
            description="Get graph visualization data for a session.",
            method="GET",
            path="/api/v1/research/sessions/{session_id}/graph",
            input_schema=_input_schema(
                path=_params_schema(
                    {
                        "session_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Research session ID",
                        }
                    },
                    required=["session_id"],
                ),
                query=_params_schema(
                    {
                        "max_nodes": {"type": "integer", "default": 200, "maximum": 500},
                    }
                ),
            ),
            output_schema=_schema_for_model(SessionGraph),
        ),
        ToolDefinition(
            name="get_session_leads",
            description="List leads in a session's queue.",
            method="GET",
            path="/api/v1/research/sessions/{session_id}/leads",
            input_schema=_input_schema(
                path=_params_schema(
                    {
                        "session_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Research session ID",
                        }
                    },
                    required=["session_id"],
                ),
                query=_params_schema(
                    {
                        "status": {"type": "string"},
                        "lead_type": {"type": "string"},
                        "limit": {"type": "integer", "default": 100, "maximum": 500},
                        "offset": {"type": "integer", "default": 0, "minimum": 0},
                    }
                ),
            ),
            output_schema=_schema_for_model(LeadsListResponse),
        ),
        ToolDefinition(
            name="skip_session_lead",
            description="Skip a pending lead with a reason.",
            method="POST",
            path="/api/v1/research/sessions/{session_id}/leads/{lead_id}/skip",
            input_schema=_input_schema(
                path=_params_schema(
                    {
                        "session_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Research session ID",
                        },
                        "lead_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Lead ID",
                        },
                    },
                    required=["session_id", "lead_id"],
                ),
                body=_schema_for_model(SkipLeadRequest),
            ),
            output_schema=_params_schema(
                {
                    "skipped": {"type": "boolean"},
                    "lead_id": {"type": "string", "format": "uuid"},
                    "reason": {"type": "string"},
                },
                required=["skipped", "lead_id", "reason"],
            ),
        ),
        ToolDefinition(
            name="prioritize_session_lead",
            description="Change priority of a pending lead.",
            method="POST",
            path="/api/v1/research/sessions/{session_id}/leads/{lead_id}/prioritize",
            input_schema=_input_schema(
                path=_params_schema(
                    {
                        "session_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Research session ID",
                        },
                        "lead_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Lead ID",
                        },
                    },
                    required=["session_id", "lead_id"],
                ),
                body=_schema_for_model(PrioritizeLeadRequest),
            ),
            output_schema=_params_schema(
                {
                    "updated": {"type": "boolean"},
                    "lead_id": {"type": "string", "format": "uuid"},
                    "new_priority": {"type": "integer"},
                },
                required=["updated", "lead_id", "new_priority"],
            ),
        ),
        ToolDefinition(
            name="requeue_session_lead",
            description="Requeue a failed or skipped lead.",
            method="POST",
            path="/api/v1/research/sessions/{session_id}/leads/{lead_id}/requeue",
            input_schema=_input_schema(
                path=_params_schema(
                    {
                        "session_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Research session ID",
                        },
                        "lead_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Lead ID",
                        },
                    },
                    required=["session_id", "lead_id"],
                ),
                query=_params_schema(
                    {
                        "priority": {"type": "integer"},
                    }
                ),
            ),
            output_schema=_params_schema(
                {
                    "requeued": {"type": "boolean"},
                    "lead_id": {"type": "string", "format": "uuid"},
                },
                required=["requeued", "lead_id"],
            ),
        ),
    ]


@router.get("", response_model=ToolListResponse)
async def list_tools() -> ToolListResponse:
    """Return minimal LLM tool definitions for research workflows."""
    return ToolListResponse(tools=_research_tools())
