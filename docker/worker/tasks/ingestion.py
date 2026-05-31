"""
Tasks — episodic memory ingestion.
"""
import logging
import os
import uuid
from datetime import datetime, timezone

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import PointStruct

from services.embedding import get_embedding

logger = logging.getLogger("cognitive-worker.ingestion")

COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "knowledge_base")


async def ingest_memory(
    qdrant: AsyncQdrantClient,
    memory_text: str,
    source: str,
    tags: list | None = None,
) -> dict:
    """
    Ingests an episodic memory into Qdrant.
    Returns a dict with id and status.
    """
    if not memory_text or not memory_text.strip():
        raise ValueError("memory_text cannot be empty")

    tags = tags or []
    point_id = str(uuid.uuid4())
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
