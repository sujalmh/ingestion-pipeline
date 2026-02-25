"""
Environment configuration for Automated Ingestion Pipeline
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)


class Settings:
    """Application settings loaded from environment variables."""
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    
    # OpenAI for LLM metadata extraction
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o-mini")
    LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0.1"))
    
    # File storage
    UPLOAD_DIRECTORY: str = os.getenv("UPLOAD_DIRECTORY", "./uploads")
    MAX_FILE_SIZE_MB: float = float(os.getenv("MAX_FILE_SIZE_MB", "100"))
    
    # Allowed file extensions
    ALLOWED_EXTENSIONS: set = {
        # Unstructured
        ".pdf", ".docx", ".doc", ".txt",
        # Structured
        ".csv", ".xlsx", ".xls", ".json", ".xml"
    }
    
    # Classification confidence threshold
    CLASSIFICATION_CONFIDENCE_THRESHOLD: float = float(
        os.getenv("CLASSIFICATION_CONFIDENCE_THRESHOLD", "0.8")
    )
    
    # API settings
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8002"))
    
    # Vector Pipeline APIs (Suyog's services)
    VECTOR_INGEST_API_URL: str = os.getenv("VECTOR_INGEST_API_URL", "http://localhost:8071")
    VECTOR_EXTRACT_API_URL: str = os.getenv("VECTOR_EXTRACT_API_URL", "http://localhost:8067")
    MILVUS_COLLECTION_NAME: str = os.getenv("MILVUS_COLLECTION_NAME", "test_user_collection")
    
    # Vector pipeline timeouts (seconds)
    VECTOR_UPLOAD_TIMEOUT: int = int(os.getenv("VECTOR_UPLOAD_TIMEOUT", "60"))
    VECTOR_PROCESS_TIMEOUT: int = int(os.getenv("VECTOR_PROCESS_TIMEOUT", "600"))
    VECTOR_INGEST_TIMEOUT: int = int(os.getenv("VECTOR_INGEST_TIMEOUT", "1200"))
    VECTOR_EXTRACT_TIMEOUT: int = int(os.getenv("VECTOR_EXTRACT_TIMEOUT", "1200"))
    
    # SQL Pipeline API
    SQL_PIPELINE_API_URL: str = os.getenv("SQL_PIPELINE_API_URL", "http://localhost:8000")
    SQL_UPLOAD_TIMEOUT: int = int(os.getenv("SQL_UPLOAD_TIMEOUT", "300"))  # 5 min for long preprocessing
    SQL_STATUS_TIMEOUT: int = int(os.getenv("SQL_STATUS_TIMEOUT", "300"))  # 5 min
    
    # Parallel processing
    PARALLEL_FILE_LIMIT: int = int(os.getenv("PARALLEL_FILE_LIMIT", "5"))
    PDF_BATCH_THRESHOLD_MB: float = float(os.getenv("PDF_BATCH_THRESHOLD_MB", "5"))
    PDF_BATCH_PAGE_COUNT: int = int(os.getenv("PDF_BATCH_PAGE_COUNT", "20"))
    
    @property
    def max_file_size_bytes(self) -> int:
        return int(self.MAX_FILE_SIZE_MB * 1024 * 1024)
    
    @property
    def pdf_batch_threshold_bytes(self) -> int:
        return int(self.PDF_BATCH_THRESHOLD_MB * 1024 * 1024)


settings = Settings()
