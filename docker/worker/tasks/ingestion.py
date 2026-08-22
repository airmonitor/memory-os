"""
Tasks — episodic memory ingestion.
"""
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import PointStruct

from services.embedding import get_embedding

_here = Path(__file__).resolve()
for _candidate in (_here.parent, *_here.parents):
    if (_candidate / "memos_config" / "loader.py").exists():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

from memos_config import config  # noqa: E402

logger = logging.getLogger("cognitive-worker.ingestion")

COLLECTION_NAME = config.qdrant.collection


async def ingest_memory(
    qdrant: AsyncQdrantClient,
    memory_text: str,
    source: str,
    tags: list | None = None,
    point_id: str | None = None,
) -> dict:
    """
    Ingests an episodic memory into Qdrant.
    Returns a dict with id and status.
    """
    if not memory_text or not memory_text.strip():
        raise ValueError("memory_text cannot be empty")

    tags = tags or []
    # uuid4 stays the DEFAULT here. A CONTENT hash was tried and rejected
    # (ADR-0002 decision 2): Qdrant's upsert replaces the payload at an
    # existing id, so two ingestions of identical text with different
    # source/tags/lifecycle fields would collapse last-writer-wins and erase
    # the first one's attribution. `ingest_memory` has exactly one caller
    # (`process_ingestion` below) — the wiki path uses `ingest_file` and the
    # reflection path builds its own `PointStruct` — and only the session
    # sweeper passes `point_id` explicitly, derived from its own job id, so a
    # replayed dispatch upserts its point instead of adding a second one.
    # Legacy points already in Qdrant were ingested before this parameter
    # existed and are NOT migrated by it.
    point_id = point_id or str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    # Generate embedding
    try:
        vector = await get_embedding(memory_text)
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        raise

    # Rich payload for search and reflection
    payload = {
        "text": memory_text,
        "source": source,
        "tags": tags,
        "created_at": timestamp,
        "reflection_count": 0,
        "last_reflected": None,
    }

    point = PointStruct(
        id=point_id,
        vector={"dense": vector},
        payload=payload,
    )

    await qdrant.upsert(
        collection_name=COLLECTION_NAME,
        points=[point],
        wait=True,
    )

    logger.info(f"Memory {point_id[:8]}... ingested ({source})")

    return {
        "id": point_id,
        "status": "ingested",
        "collection": COLLECTION_NAME,
    }
