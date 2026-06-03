"""
Qdrant client — connection and collection validation.
Creates a hybrid collection (dense + BM25 sparse) if it doesn't exist.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, Modifier, SparseVectorParams, VectorParams

_here = Path(__file__).resolve()
for _candidate in (_here.parent, *_here.parents):
    if (_candidate / "memos_config" / "loader.py").exists():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

from memos_config import config  # noqa: E402

logger = logging.getLogger("cognitive-worker.qdrant")

_client: AsyncQdrantClient | None = None


def get_qdrant_client() -> AsyncQdrantClient:
    """Return a singleton AsyncQdrantClient."""
    global _client
    if _client is None:
        q = config.qdrant
        _client = AsyncQdrantClient(
            host=q.host,
            port=int(q.port),
            api_key=q.api_key or None,
            https=False,
        )
        logger.info(f"Connected to Qdrant at {q.host}:{q.port}")
    return _client


async def ensure_collection(client: AsyncQdrantClient) -> None:
    """Ensure the hybrid collection exists with dense + sparse configs."""
    collection = config.qdrant.collection
    dims = int(config.litellm.models.embedding.dimensions)
    try:
        collections = (await client.get_collections()).collections
        names = [c.name for c in collections]
        if collection not in names:
            logger.info(
                f"Creating collection {collection} with dense={dims} dims + sparse BM25"
            )
            await client.create_collection(
                collection_name=collection,
                vectors_config={
                    "dense": VectorParams(size=dims, distance=Distance.COSINE),
                },
                sparse_vectors_config={
                    "sparse": SparseVectorParams(modifier=Modifier.IDF),
                },
            )
        else:
            logger.info(f"Collection {collection} already exists")
    except Exception as e:
        logger.error(f"Error validating collection: {e}")
        raise
