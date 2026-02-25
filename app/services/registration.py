"""
Registration Service

Handles file registration into the inbound_files_metadata table.
This is the entry point for all files entering the pipeline.

Process:
1. Calculate SHA256 hash
2. Check for duplicates
3. Store file with UUID naming
4. Create entry in inbound_files_metadata
5. Return tracking ID
"""
import uuid
import aiofiles
from pathlib import Path
from typing import Optional

from app.config import settings
from app.models.db import (
    insert_inbound_file,
    check_duplicate_hash,
)
from app.models.schemas import (
    SourceType,
    FileStatus,
    UploadResponse,
)
from app.utils.file_utils import (
    calculate_sha256,
    get_file_extension,
    get_mime_type_from_content,
    generate_stored_filename,
)


class RegistrationService:
    """
    Service for registering incoming files into the pipeline.
    """

    def __init__(self, upload_directory: Optional[str] = None):
        self.upload_directory = Path(
            upload_directory or settings.UPLOAD_DIRECTORY
        )
        self.upload_directory.mkdir(parents=True, exist_ok=True)

    async def register_file(
        self,
        content: bytes,
        original_filename: str,
        source_type: SourceType = SourceType.DIRECT_UPLOAD,
        source_identifier: Optional[str] = None,
        source_url: Optional[str] = None,
    ) -> UploadResponse:
        """
        Register a file in the ingestion pipeline.

        Args:
            content: File content as bytes
            original_filename: Original name of the file
            source_type: Origin of the file
            source_identifier: Specific source ID (user ID, agent ID, etc.)
            source_url: Original URL if downloaded from somewhere

        Returns:
            UploadResponse with file_id and status
        """
        # Step 1: Validate file
        validation_error = self._validate_file(content, original_filename)
        if validation_error:
            return UploadResponse(
                success=False,
                message=validation_error,
                filename=original_filename,
            )

        # Step 2: Calculate SHA256 hash
        file_hash = calculate_sha256(content)

        # Step 3: Check for duplicates
        existing = await check_duplicate_hash(file_hash)
        if existing:
            return UploadResponse(
                success=False,
                message="File is a duplicate of an existing record.",
                filename=original_filename,
                is_duplicate=True,
                existing_file_id=existing["id"],
                status=existing["status"],
            )

        # Step 4: Generate file ID and stored filename
        file_id = str(uuid.uuid4())
        file_extension = get_file_extension(original_filename)
        stored_filename = generate_stored_filename(original_filename, file_id)
        file_path = self.upload_directory / stored_filename

        # Step 5: Store file
        try:
            async with aiofiles.open(file_path, "wb") as f:
                await f.write(content)
        except Exception as e:
            return UploadResponse(
                success=False,
                message=f"Failed to store file: {str(e)}",
                filename=original_filename,
            )

        # Step 6: Get MIME type
        mime_type = get_mime_type_from_content(content, original_filename)

        # Step 7: Create entry in inbound_files_metadata
        try:
            db_file_id = await insert_inbound_file(
                source_type=source_type.value,
                original_filename=original_filename,
                stored_filename=stored_filename,
                file_path=str(file_path.resolve()),
                file_size=len(content),
                file_extension=file_extension,
                sha256_hash=file_hash,
                source_identifier=source_identifier,
                source_url=source_url,
                mime_type=mime_type,
            )
        except Exception as e:
            # Clean up stored file on DB error
            if file_path.exists():
                file_path.unlink()
            return UploadResponse(
                success=False,
                message=f"Failed to register file: {str(e)}",
                filename=original_filename,
            )

        return UploadResponse(
            success=True,
            message="File registered successfully.",
            file_id=db_file_id,
            filename=original_filename,
            status=FileStatus.OPEN_REQUEST.value,
        )

    def _validate_file(self, content: bytes, filename: str) -> Optional[str]:
        """
        Validate file before processing.

        Returns:
            Error message if validation fails, None if valid.
        """
        # Check if file is empty
        if not content:
            return "File is empty."

        # Check file size
        if len(content) > settings.max_file_size_bytes:
            max_mb = settings.MAX_FILE_SIZE_MB
            actual_mb = len(content) / (1024 * 1024)
            return (
                f"File size ({actual_mb:.1f} MB) exceeds "
                f"maximum allowed ({max_mb} MB)."
            )

        # Check file extension
        extension = get_file_extension(filename)
        if extension and extension not in settings.ALLOWED_EXTENSIONS:
            return f"File extension '{extension}' is not allowed."

        return None


# Singleton instance
registration_service = RegistrationService()


async def register_uploaded_file(
    content: bytes,
    original_filename: str,
    source_type: SourceType = SourceType.DIRECT_UPLOAD,
    source_identifier: Optional[str] = None,
    source_url: Optional[str] = None,
) -> UploadResponse:
    """
    Convenience function to register a file.

    This is the main entry point for file registration.
    """
    return await registration_service.register_file(
        content=content,
        original_filename=original_filename,
        source_type=source_type,
        source_identifier=source_identifier,
        source_url=source_url,
    )
