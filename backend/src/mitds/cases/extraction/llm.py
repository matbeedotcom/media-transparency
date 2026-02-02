"""LLM-based entity extraction.

Optional LLM extraction for enhanced entity recognition from
unstructured text. Supports OpenAI and Anthropic providers.
"""

import json
import logging
from dataclasses import dataclass
from typing import Any

from ...config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class LLMExtractedEntity:
    """An entity extracted via LLM."""

    entity_type: str  # organization, person
    value: str
    confidence: float
    context: str | None = None
    reasoning: str | None = None


class LLMExtractor:
    """LLM-based entity extractor.

    Uses an LLM to identify entities in text that deterministic
    patterns might miss, such as:
    - Organization names without legal suffixes
    - Person names with roles
    - Relationship claims (funded by, owned by)
    """

    EXTRACTION_PROMPT = """Analyze the following text and extract all mentioned organizations and people.

For each entity found, provide:
- type: "organization" or "person"
- name: The full name as mentioned
- confidence: A score from 0.0 to 1.0 indicating how confident you are
- context: Brief description of why this entity is relevant

Return a JSON array of entities. Only include entities that are actually mentioned in the text.
Do not make up or infer entities that aren't explicitly stated.

Text to analyze:
---
{text}
---

Return ONLY valid JSON, no other text:
[
  {{"type": "organization", "name": "Example Corp", "confidence": 0.9, "context": "Mentioned as funder"}}
]"""

    def __init__(self, provider: str = "openai"):
        """Initialize the LLM extractor.

        Args:
            provider: LLM provider ('openai' or 'anthropic')
        """
        self.provider = provider
        self._client: Any = None

    async def _get_client(self) -> Any:
        """Get or create the LLM client."""
        if self._client is not None:
            return self._client

        settings = get_settings()

        if self.provider == "openai":
            try:
                import openai
                self._client = openai.AsyncOpenAI(
                    api_key=settings.openai_api_key,
                )
            except ImportError:
                raise RuntimeError("openai package not installed")
        elif self.provider == "anthropic":
            try:
                import anthropic
                self._client = anthropic.AsyncAnthropic(
                    api_key=settings.anthropic_api_key,
                )
            except ImportError:
                raise RuntimeError("anthropic package not installed")
        else:
            raise ValueError(f"Unknown LLM provider: {self.provider}")

        return self._client

    async def extract(self, text: str) -> list[LLMExtractedEntity]:
        """Extract entities from text using LLM.

        Args:
            text: The text to extract entities from

        Returns:
            List of extracted entities
        """
        # Truncate text if too long
        max_chars = 8000
        if len(text) > max_chars:
            text = text[:max_chars] + "\n...[truncated]..."

        try:
            client = await self._get_client()

            if self.provider == "openai":
                response = await self._extract_openai(client, text)
            else:
                response = await self._extract_anthropic(client, text)

            return self._parse_response(response)

        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return []

    async def _extract_openai(self, client: Any, text: str) -> str:
        """Extract using OpenAI."""
        prompt = self.EXTRACTION_PROMPT.format(text=text)

        response = await client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {
                    "role": "system",
                    "content": "You are an entity extraction assistant. Return only valid JSON.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=2000,
        )

        return response.choices[0].message.content

    async def _extract_anthropic(self, client: Any, text: str) -> str:
        """Extract using Anthropic."""
        prompt = self.EXTRACTION_PROMPT.format(text=text)

        response = await client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=2000,
            messages=[
                {"role": "user", "content": prompt},
            ],
        )

        return response.content[0].text

    def _parse_response(self, response: str) -> list[LLMExtractedEntity]:
        """Parse the LLM response into entities."""
        entities: list[LLMExtractedEntity] = []

        try:
            # Clean up response (remove markdown code blocks if present)
            response = response.strip()
            if response.startswith("```"):
                lines = response.split("\n")
                response = "\n".join(lines[1:-1])

            data = json.loads(response)

            if not isinstance(data, list):
                logger.warning("LLM response is not a list")
                return entities

            for item in data:
                if not isinstance(item, dict):
                    continue

                entity_type = item.get("type", "").lower()
                if entity_type not in ("organization", "person"):
                    continue

                name = item.get("name", "").strip()
                if not name or len(name) < 2:
                    continue

                confidence = float(item.get("confidence", 0.7))
                # Cap LLM confidence at 0.8 since it's less reliable
                confidence = min(confidence, 0.8)

                entities.append(LLMExtractedEntity(
                    entity_type=entity_type,
                    value=name,
                    confidence=confidence,
                    context=item.get("context"),
                    reasoning=item.get("reasoning"),
                ))

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse LLM response as JSON: {e}")
        except Exception as e:
            logger.warning(f"Error parsing LLM response: {e}")

        return entities


# Factory function
def get_llm_extractor(provider: str | None = None) -> LLMExtractor:
    """Get an LLM extractor.

    Args:
        provider: LLM provider ('openai' or 'anthropic').
                 If not specified, uses settings.

    Returns:
        LLMExtractor instance
    """
    if provider is None:
        settings = get_settings()
        provider = getattr(settings, "llm_provider", "openai")

    return LLMExtractor(provider=provider)
