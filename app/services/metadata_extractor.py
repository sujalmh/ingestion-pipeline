"""
LLM Metadata Extractor Service

Uses OpenAI GPT-4o-mini to extract base-level metadata from files:
- major_domain: High-level category (Economy, Finance, Agriculture, etc.)
- sub_domain: Specific area within domain
- brief_summary: 2-3 sentence description of content

This metadata is stored in the operational_metadata table.
"""
import os
import json
from typing import Optional, Tuple
from pathlib import Path

from app.config import settings
from app.models.schemas import LLMMetadataResult


def _fallback_load_type(filename_lower: str) -> str:
    """Heuristic: infer one_time vs incremental from filename."""
    incremental_keywords = ("incremental", "delta", "append", "update", "monthly", "daily", "weekly")
    if any(kw in filename_lower for kw in incremental_keywords):
        return "incremental"
    return "one_time"


# Domain taxonomy for guidance
DOMAIN_TAXONOMY = """
Major Domains:
- Economy: Macro Policy, Inflation, GDP & Growth, Trade, Fiscal Policy
- Finance: Banking, Insurance, Capital Markets, NBFCs, Monetary Policy
- Agriculture: Crops, Livestock, Rural Economy, Food Security
- Industry: Manufacturing, Infrastructure, Energy, Mining
- Social: Employment, Education, Health, Demographics
- Government: Budget, Taxation, Public Policy, Regulations
- Technology: Digital Infrastructure, IT Services, Innovation
- Environment: Climate, Sustainability, Natural Resources
"""


def get_extraction_prompt(filename: str, content_sample: str) -> str:
    """
    Generate the prompt for LLM metadata extraction.
    """
    return f"""You are an expert at analyzing documents to extract structured metadata.

TASK: Analyze the following document and extract metadata.

DOCUMENT FILENAME: {filename}

CONTENT SAMPLE:
{content_sample}

DOMAIN TAXONOMY (use these categories):
{DOMAIN_TAXONOMY}

INSTRUCTIONS:
1. Identify the major_domain from the taxonomy above
2. Identify a specific sub_domain within that major domain
3. Write a brief 2-3 sentence summary of the document
4. Set load_type: "one_time" if this is a standalone/full snapshot dataset (new table or full refresh). Set load_type: "incremental" if this appears to be an update, append, delta, or recurring feed (e.g. monthly/daily updates, additions to an existing table).

IMPORTANT:
- Be specific and accurate based on the content
- If unclear, infer from filename and structure
- Keep the summary concise but informative
- For load_type: prefer "incremental" when filename or content suggests updates/appends (e.g. "monthly", "daily", "delta", "update", "append")

Respond with ONLY a valid JSON object in this exact format:
{{
    "major_domain": "string",
    "sub_domain": "string",
    "brief_summary": "string",
    "load_type": "one_time or incremental"
}}"""


class MetadataExtractor:
    """
    Extracts metadata from files using LLM.
    """

    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        self.model = settings.LLM_MODEL
        self.temperature = settings.LLM_TEMPERATURE

    async def extract_metadata(
        self,
        filename: str,
        content_sample: str,
    ) -> LLMMetadataResult:
        """
        Extract metadata using LLM.

        Args:
            filename: Name of the file
            content_sample: Sample of file content (first ~4000 chars)

        Returns:
            LLMMetadataResult with domain, subdomain, summary
        """
        if not self.api_key:
            # Fallback if no API key
            return self._fallback_extraction(filename)

        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=self.api_key)

            prompt = get_extraction_prompt(filename, content_sample)

            response = await client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a metadata extraction assistant. "
                                   "Always respond with valid JSON only.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=self.temperature,
                max_tokens=500,
            )

            result_text = response.choices[0].message.content.strip()

            # Parse JSON response
            result = self._parse_llm_response(result_text)
            if result:
                return result

            # Fallback if parsing fails
            return self._fallback_extraction(filename)

        except Exception as e:
            print(f"LLM extraction error: {e}")
            return self._fallback_extraction(filename)

    def _parse_llm_response(
        self, response_text: str
    ) -> Optional[LLMMetadataResult]:
        """
        Parse LLM response and extract JSON.
        """
        try:
            # Clean up response
            text = response_text.strip()

            # Handle markdown code blocks
            if "```json" in text:
                start = text.find("```json") + 7
                end = text.find("```", start)
                if end != -1:
                    text = text[start:end].strip()
            elif "```" in text:
                start = text.find("```") + 3
                end = text.find("```", start)
                if end != -1:
                    text = text[start:end].strip()

            # Find JSON object
            brace_start = text.find("{")
            brace_end = text.rfind("}") + 1
            if brace_start != -1 and brace_end > brace_start:
                text = text[brace_start:brace_end]

            data = json.loads(text)

            load_type_raw = (data.get("load_type") or "one_time").strip().lower()
            load_type = "incremental" if load_type_raw == "incremental" else "one_time"
            return LLMMetadataResult(
                major_domain=data.get("major_domain", "General"),
                sub_domain=data.get("sub_domain", "General"),
                brief_summary=data.get(
                    "brief_summary", "No summary available"
                ),
                load_type=load_type,
            )

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Failed to parse LLM response: {e}")
            print(f"Response was: {response_text[:500]}")
            return None

    def _fallback_extraction(self, filename: str) -> LLMMetadataResult:
        """
        Fallback extraction when LLM is unavailable.

        Uses filename heuristics to guess domain.
        """
        filename_lower = filename.lower()

        # Simple keyword matching
        domain_keywords = {
            "Economy": ["economic", "economy", "gdp", "growth", "inflation"],
            "Finance": ["finance", "financial", "bank", "credit", "monetary"],
            "Agriculture": ["agriculture", "agri", "crop", "farm", "rural"],
            "Industry": ["industry", "manufacturing", "infrastructure"],
            "Government": ["budget", "government", "policy", "ministry"],
        }

        for domain, keywords in domain_keywords.items():
            if any(kw in filename_lower for kw in keywords):
                return LLMMetadataResult(
                    major_domain=domain,
                    sub_domain="General",
                    brief_summary=f"Document: {filename}",
                    load_type=_fallback_load_type(filename_lower),
                )

        return LLMMetadataResult(
            major_domain="General",
            sub_domain="General",
            brief_summary=f"Document: {filename}",
            load_type=_fallback_load_type(filename_lower),
        )


# Singleton instance
metadata_extractor = MetadataExtractor()


async def extract_file_metadata(
    filename: str,
    content_sample: str,
) -> LLMMetadataResult:
    """
    Convenience function to extract metadata.
    """
    return await metadata_extractor.extract_metadata(filename, content_sample)
