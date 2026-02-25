"""
File utility functions for the ingestion pipeline.
"""
import hashlib
import mimetypes
from pathlib import Path
from typing import Optional


def calculate_sha256(content: bytes) -> str:
    """
    Calculate SHA256 hash of file content.
    
    Args:
        content: File content as bytes.
        
    Returns:
        SHA256 hash as hex string.
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update(content)
    return sha256_hash.hexdigest()


def calculate_sha256_file(file_path: str) -> str:
    """
    Calculate SHA256 hash of a file from its path.
    
    Args:
        file_path: Path to the file.
        
    Returns:
        SHA256 hash as hex string.
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def get_file_extension(filename: str) -> str:
    """
    Get the file extension (lowercase, with dot).
    
    Args:
        filename: Name of the file.
        
    Returns:
        File extension in lowercase (e.g., ".pdf", ".csv").
    """
    return Path(filename).suffix.lower()


def get_mime_type(filename: str) -> Optional[str]:
    """
    Get the MIME type based on filename.
    
    Args:
        filename: Name of the file.
        
    Returns:
        MIME type string or None if unknown.
    """
    mime_type, _ = mimetypes.guess_type(filename)
    return mime_type


def get_mime_type_from_content(content: bytes, filename: str) -> Optional[str]:
    """
    Get MIME type by checking file magic bytes.
    Falls back to extension-based detection.
    
    Args:
        content: File content as bytes.
        filename: Name of the file (for fallback).
        
    Returns:
        MIME type string or None if unknown.
    """
    # Magic bytes signatures
    signatures = {
        b'%PDF': 'application/pdf',
        b'PK\x03\x04': 'application/zip',  # Also covers .xlsx, .docx
        b'\xd0\xcf\x11\xe0': 'application/msword',  # .doc, .xls (OLE)
        b'{\n': 'application/json',
        b'[': 'application/json',
        b'<?xml': 'application/xml',
    }
    
    # Check magic bytes
    for signature, mime in signatures.items():
        if content.startswith(signature):
            # Refine for Office formats
            if mime == 'application/zip':
                ext = get_file_extension(filename)
                if ext == '.xlsx':
                    return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                elif ext == '.docx':
                    return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            return mime
    
    # Fallback to extension-based detection
    return get_mime_type(filename)


def is_text_file(content: bytes) -> bool:
    """
    Check if content appears to be text (for CSV detection).
    
    Args:
        content: File content as bytes.
        
    Returns:
        True if content appears to be text.
    """
    try:
        # Try to decode as UTF-8
        sample = content[:8192]
        sample.decode('utf-8')
        return True
    except UnicodeDecodeError:
        return False


def generate_stored_filename(original_filename: str, file_id: str) -> str:
    """
    Generate a unique stored filename.
    
    Args:
        original_filename: Original name of the file.
        file_id: Unique file identifier.
        
    Returns:
        Stored filename in format: {file_id}{extension}
    """
    extension = get_file_extension(original_filename)
    return f"{file_id}{extension}"
