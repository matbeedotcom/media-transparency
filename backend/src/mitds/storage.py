"""S3-compatible storage client for MITDS.

Provides utilities for storing and retrieving raw data files.
"""

import hashlib
from datetime import datetime
from io import BytesIO
from typing import BinaryIO

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import get_settings


class StorageClient:
    """S3-compatible storage client for raw file storage.

    Supports MinIO for local development and AWS S3 for production.
    """

    def __init__(self):
        settings = get_settings()
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )
        self._bucket = settings.s3_bucket

    def upload_file(
        self,
        data: bytes | BinaryIO,
        key: str,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Upload a file to storage.

        Args:
            data: File content as bytes or file-like object
            key: S3 key (path within bucket)
            content_type: MIME type of the content
            metadata: Optional metadata to attach to the object

        Returns:
            Full S3 path (s3://bucket/key)
        """
        if isinstance(data, bytes):
            data = BytesIO(data)

        extra_args = {"ContentType": content_type}
        if metadata:
            extra_args["Metadata"] = metadata

        self._client.upload_fileobj(
            data,
            self._bucket,
            key,
            ExtraArgs=extra_args,
        )

        return f"s3://{self._bucket}/{key}"

    def download_file(self, key: str) -> bytes:
        """Download a file from storage.

        Args:
            key: S3 key (path within bucket)

        Returns:
            File content as bytes
        """
        buffer = BytesIO()
        self._client.download_fileobj(self._bucket, key, buffer)
        buffer.seek(0)
        return buffer.read()

    def file_exists(self, key: str) -> bool:
        """Check if a file exists in storage.

        Args:
            key: S3 key (path within bucket)

        Returns:
            True if file exists, False otherwise
        """
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def get_file_metadata(self, key: str) -> dict:
        """Get metadata for a file.

        Args:
            key: S3 key (path within bucket)

        Returns:
            Object metadata including size, content type, etc.
        """
        response = self._client.head_object(Bucket=self._bucket, Key=key)
        return {
            "content_type": response.get("ContentType"),
            "content_length": response.get("ContentLength"),
            "last_modified": response.get("LastModified"),
            "metadata": response.get("Metadata", {}),
        }

    def delete_file(self, key: str) -> None:
        """Delete a file from storage.

        Args:
            key: S3 key (path within bucket)
        """
        self._client.delete_object(Bucket=self._bucket, Key=key)

    def list_files(self, prefix: str = "", max_keys: int = 1000) -> list[str]:
        """List files in storage with a given prefix.

        Args:
            prefix: Key prefix to filter by
            max_keys: Maximum number of keys to return

        Returns:
            List of S3 keys
        """
        response = self._client.list_objects_v2(
            Bucket=self._bucket,
            Prefix=prefix,
            MaxKeys=max_keys,
        )

        keys = []
        for obj in response.get("Contents", []):
            keys.append(obj["Key"])

        return keys

    def generate_presigned_url(
        self, key: str, expiration: int = 3600, method: str = "get_object"
    ) -> str:
        """Generate a presigned URL for temporary access.

        Args:
            key: S3 key (path within bucket)
            expiration: URL expiration time in seconds
            method: S3 operation ('get_object' or 'put_object')

        Returns:
            Presigned URL
        """
        return self._client.generate_presigned_url(
            method,
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=expiration,
        )


def compute_content_hash(data: bytes) -> str:
    """Compute SHA-256 hash of content.

    Args:
        data: Content to hash

    Returns:
        Hex-encoded SHA-256 hash
    """
    return hashlib.sha256(data).hexdigest()


def generate_storage_key(
    source: str,
    identifier: str,
    extension: str = "json",
    timestamp: datetime | None = None,
) -> str:
    """Generate a storage key for a raw data file.

    Format: {source}/{year-month}/{identifier}.{extension}

    Args:
        source: Data source name (e.g., 'irs990', 'cra')
        identifier: Unique identifier for the record
        extension: File extension
        timestamp: Optional timestamp for partitioning (defaults to now)

    Returns:
        S3 key path
    """
    if timestamp is None:
        timestamp = datetime.utcnow()

    year_month = timestamp.strftime("%Y-%m")

    # Sanitize identifier for use in path
    safe_identifier = identifier.replace("/", "_").replace("\\", "_")

    return f"{source}/{year_month}/{safe_identifier}.{extension}"


# Singleton instance
_storage_client: StorageClient | None = None


def get_storage() -> StorageClient:
    """Get the storage client singleton."""
    global _storage_client
    if _storage_client is None:
        _storage_client = StorageClient()
    return _storage_client


# =============================================================================
# Evidence Storage Helpers (T015 - Case Intake System)
# =============================================================================


def generate_evidence_key(
    case_id: str,
    evidence_id: str,
    filename: str = "content",
    extension: str = "bin",
) -> str:
    """Generate a storage key for case evidence.

    Format: evidence/{case_id}/{evidence_id}/{filename}.{extension}

    Args:
        case_id: The case ID (UUID as string)
        evidence_id: The evidence ID (UUID as string)
        filename: Base filename (default: 'content')
        extension: File extension (default: 'bin')

    Returns:
        S3 key path for the evidence
    """
    return f"evidence/{case_id}/{evidence_id}/{filename}.{extension}"


async def store_evidence_content(
    case_id: str,
    evidence_id: str,
    content: bytes,
    content_type: str = "application/octet-stream",
    filename: str = "content",
    extension: str | None = None,
    metadata: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Store evidence content in S3 and return the key and hash.

    Args:
        case_id: The case ID
        evidence_id: The evidence ID
        content: The raw content bytes
        content_type: MIME type of the content
        filename: Base filename for storage
        extension: File extension (inferred from content_type if not provided)
        metadata: Optional metadata to attach

    Returns:
        Tuple of (S3 key path, SHA-256 content hash)
    """
    # Infer extension from content type if not provided
    if extension is None:
        extension = _content_type_to_extension(content_type)

    key = generate_evidence_key(case_id, evidence_id, filename, extension)
    content_hash = compute_content_hash(content)

    # Add hash to metadata
    full_metadata = metadata or {}
    full_metadata["content_hash"] = content_hash
    full_metadata["case_id"] = case_id
    full_metadata["evidence_id"] = evidence_id

    storage = get_storage()
    storage.upload_file(content, key, content_type, full_metadata)

    return key, content_hash


def _content_type_to_extension(content_type: str) -> str:
    """Map MIME type to file extension."""
    mapping = {
        "text/html": "html",
        "text/plain": "txt",
        "application/json": "json",
        "application/xml": "xml",
        "text/xml": "xml",
        "application/pdf": "pdf",
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/gif": "gif",
    }
    return mapping.get(content_type, "bin")


async def retrieve_evidence_content(key: str) -> bytes:
    """Retrieve evidence content from S3.

    Args:
        key: The S3 key path

    Returns:
        The raw content bytes
    """
    storage = get_storage()
    return storage.download_file(key)


async def delete_evidence_content(case_id: str, evidence_id: str) -> None:
    """Delete all evidence content for a specific evidence record.

    Args:
        case_id: The case ID
        evidence_id: The evidence ID
    """
    storage = get_storage()
    prefix = f"evidence/{case_id}/{evidence_id}/"
    keys = storage.list_files(prefix)
    for key in keys:
        storage.delete_file(key)


async def delete_case_evidence(case_id: str) -> int:
    """Delete all evidence content for a case.

    Args:
        case_id: The case ID

    Returns:
        Number of files deleted
    """
    storage = get_storage()
    prefix = f"evidence/{case_id}/"
    keys = storage.list_files(prefix)
    for key in keys:
        storage.delete_file(key)
    return len(keys)
