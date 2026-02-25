# -*- coding: utf-8 -*-
"""
PDF Splitter Service

Splits large PDFs into smaller page-range batches for parallel processing.
Uses PyMuPDF (fitz) for fast, reliable PDF manipulation.
"""

import logging
import tempfile
from pathlib import Path
from typing import List

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


def get_pdf_page_count(file_path: str) -> int:
    """
    Get the total number of pages in a PDF.

    Args:
        file_path: Path to the PDF file.

    Returns:
        Total page count.
    """
    doc = fitz.open(file_path)
    count = len(doc)
    doc.close()
    return count


def split_pdf(file_path: str, pages_per_batch: int) -> List[str]:
    """
    Split a PDF into smaller temporary PDFs of N pages each.

    The last batch may have fewer than N pages.

    Args:
        file_path: Path to the source PDF.
        pages_per_batch: Maximum pages per split file.

    Returns:
        List of paths to the split PDF temp files.
        Caller is responsible for cleaning up these files.
    """
    src = fitz.open(file_path)
    total = len(src)

    if total <= pages_per_batch:
        # No splitting needed -- return the original path
        src.close()
        logger.info(
            "PDF %s has %d pages (<= %d), skipping split",
            file_path, total, pages_per_batch,
        )
        return [file_path]

    stem = Path(file_path).stem
    batch_paths: List[str] = []

    for start in range(0, total, pages_per_batch):
        end = min(start + pages_per_batch, total)  # exclusive

        batch_doc = fitz.open()  # new empty PDF
        batch_doc.insert_pdf(src, from_page=start, to_page=end - 1)

        # Write to a temp file that persists until caller deletes it
        tmp = tempfile.NamedTemporaryFile(
            prefix=f"{stem}_batch{start}-{end}_",
            suffix=".pdf",
            delete=False,
        )
        batch_doc.save(tmp.name)
        batch_doc.close()
        tmp.close()

        batch_paths.append(tmp.name)
        logger.info(
            "Created batch %s (pages %d-%d of %d)",
            tmp.name, start + 1, end, total,
        )

    src.close()
    logger.info(
        "Split %s (%d pages) into %d batches of up to %d pages",
        file_path, total, len(batch_paths), pages_per_batch,
    )
    return batch_paths
