"""
File Classifier Service

Determines if a file is structured or unstructured based on:
1. File extension
2. MIME type
3. Content sampling (for ambiguous types)

Classification Rules:
- Unstructured: PDF, DOCX, DOC, TXT
- Structured: CSV, XLSX, XLS
- Analyze: JSON, XML (check content structure)
"""
from typing import Optional

from app.models.schemas import Classification, ClassificationResult
from app.utils.file_utils import get_file_extension, get_mime_type


# Classification mappings
EXTENSION_CLASSIFICATION = {
    # Unstructured
    ".pdf": (Classification.UNSTRUCTURED, 0.95),
    ".docx": (Classification.UNSTRUCTURED, 0.95),
    ".doc": (Classification.UNSTRUCTURED, 0.95),
    ".txt": (Classification.UNSTRUCTURED, 0.90),
    # Structured
    ".csv": (Classification.STRUCTURED, 0.95),
    ".xlsx": (Classification.STRUCTURED, 0.95),
    ".xls": (Classification.STRUCTURED, 0.95),
    # Ambiguous - need content analysis
    ".json": (Classification.UNKNOWN, 0.80),
    ".xml": (Classification.UNKNOWN, 0.80),
}

MIME_CLASSIFICATION = {
    # Unstructured
    "application/pdf": Classification.UNSTRUCTURED,
    "application/msword": Classification.UNSTRUCTURED,
    "application/vnd.openxmlformats-officedocument"
    ".wordprocessingml.document": Classification.UNSTRUCTURED,
    "text/plain": Classification.UNSTRUCTURED,
    # Structured
    "text/csv": Classification.STRUCTURED,
    "application/vnd.ms-excel": Classification.STRUCTURED,
    "application/vnd.openxmlformats-officedocument"
    ".spreadsheetml.sheet": Classification.STRUCTURED,
    # Ambiguous
    "application/json": Classification.UNKNOWN,
    "application/xml": Classification.UNKNOWN,
    "text/xml": Classification.UNKNOWN,
}


class FileClassifier:
    """
    Classifies files as structured or unstructured.
    """

    def classify(
        self,
        filename: str,
        mime_type: Optional[str] = None,
        content: Optional[bytes] = None,
    ) -> ClassificationResult:
        """
        Classify a file based on extension and MIME type.

        Args:
            filename: Name of the file
            mime_type: MIME type if already detected
            content: File content for ambiguous types

        Returns:
            ClassificationResult with classification and confidence
        """
        extension = get_file_extension(filename)
        detected_mime = mime_type or get_mime_type(filename)

        # Step 1: Try extension-based classification
        if extension in EXTENSION_CLASSIFICATION:
            classification, confidence = EXTENSION_CLASSIFICATION[extension]

            # If not ambiguous, return immediately
            if classification != Classification.UNKNOWN:
                return ClassificationResult(
                    classification=classification,
                    confidence=confidence,
                    file_extension=extension,
                    mime_type=detected_mime,
                    reason=f"Extension-based: {extension}",
                )

        # Step 2: Try MIME-based classification
        if detected_mime and detected_mime in MIME_CLASSIFICATION:
            mime_classification = MIME_CLASSIFICATION[detected_mime]

            if mime_classification != Classification.UNKNOWN:
                return ClassificationResult(
                    classification=mime_classification,
                    confidence=0.90,
                    file_extension=extension,
                    mime_type=detected_mime,
                    reason=f"MIME-based: {detected_mime}",
                )

        # Step 3: Content-based classification for ambiguous types
        if content and extension in (".json", ".xml"):
            return self._classify_by_content(
                content, extension, detected_mime
            )

        # Step 4: Default to unstructured with low confidence
        return ClassificationResult(
            classification=Classification.UNSTRUCTURED,
            confidence=0.60,
            file_extension=extension,
            mime_type=detected_mime,
            reason="Default classification - unknown type",
        )

    def _classify_by_content(
        self,
        content: bytes,
        extension: str,
        mime_type: Optional[str],
    ) -> ClassificationResult:
        """
        Classify ambiguous file types by analyzing content.

        For JSON/XML, check if it looks like tabular data.
        """
        try:
            text = content[:8192].decode("utf-8", errors="ignore")

            if extension == ".json":
                return self._classify_json(text, extension, mime_type)
            elif extension == ".xml":
                return self._classify_xml(text, extension, mime_type)

        except Exception:
            pass

        # Default for ambiguous
        return ClassificationResult(
            classification=Classification.UNSTRUCTURED,
            confidence=0.70,
            file_extension=extension,
            mime_type=mime_type,
            reason="Content analysis inconclusive",
        )

    def _classify_json(
        self,
        text: str,
        extension: str,
        mime_type: Optional[str],
    ) -> ClassificationResult:
        """
        Classify JSON file.

        - Array of objects with consistent keys = Structured
        - Nested/complex structure = Unstructured
        """
        import json

        try:
            data = json.loads(text)

            # Array of objects = likely tabular data
            if isinstance(data, list) and len(data) > 0:
                if all(isinstance(item, dict) for item in data[:10]):
                    # Check if keys are consistent
                    first_keys = set(data[0].keys())
                    consistent = all(
                        set(item.keys()) == first_keys
                        for item in data[:10]  # noqa: E501
                    )
                    if consistent:
                        return ClassificationResult(
                            classification=Classification.STRUCTURED,
                            confidence=0.85,
                            file_extension=extension,
                            mime_type=mime_type,
                            reason="JSON array of objects with consistent keys",
                        )

            # Single object or nested = unstructured
            return ClassificationResult(
                classification=Classification.UNSTRUCTURED,
                confidence=0.75,
                file_extension=extension,
                mime_type=mime_type,
                reason="JSON with complex/nested structure",
            )

        except json.JSONDecodeError:
            return ClassificationResult(
                classification=Classification.UNSTRUCTURED,
                confidence=0.60,
                file_extension=extension,
                mime_type=mime_type,
                reason="Invalid JSON - treating as text",
            )

    def _classify_xml(
        self,
        text: str,
        extension: str,
        mime_type: Optional[str],
    ) -> ClassificationResult:
        """
        Classify XML file.

        Simple heuristic: if it has repeating elements, likely structured.
        """
        # Simple heuristic: count repeating tags
        import re

        tags = re.findall(r"<(\w+)", text)
        if tags:
            from collections import Counter

            tag_counts = Counter(tags)
            # If any tag repeats more than 5 times, likely tabular
            max_count = max(tag_counts.values())
            if max_count > 5:
                return ClassificationResult(
                    classification=Classification.STRUCTURED,
                    confidence=0.80,
                    file_extension=extension,
                    mime_type=mime_type,
                    reason="XML with repeating elements",
                )

        return ClassificationResult(
            classification=Classification.UNSTRUCTURED,
            confidence=0.75,
            file_extension=extension,
            mime_type=mime_type,
            reason="XML document structure",
        )


# Singleton instance
file_classifier = FileClassifier()


def classify_file(
    filename: str,
    mime_type: Optional[str] = None,
    content: Optional[bytes] = None,
) -> ClassificationResult:
    """
    Convenience function to classify a file.
    """
    return file_classifier.classify(filename, mime_type, content)
