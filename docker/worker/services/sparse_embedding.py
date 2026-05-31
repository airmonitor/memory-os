"""
BM25 Sparse Embedding client via FastEmbed.
Caches the model in memory (lazy init).
"""
import logging
from fastembed.sparse import SparseTextEmbedding

logger = logging.getLogger("cognitive-worker.sparse_embedding")

BM25_MODEL = "Qdrant/bm25"
_model = None


def _get_model() -> SparseTextEmbedding:
    """Lazy init of the FastEmbed BM25 model."""
    global _model
    if _model is None:
        logger.info("Loading BM25 sparse embedding model...")
        _model = SparseTextEmbedding(model_name=BM25_MODEL)
        logger.info("BM25 model loaded.")
    return _model


def get_sparse_embedding(text: str) -> dict:
    """
    Generates BM25 sparse embedding via FastEmbed.
    Returns a Qdrant-compatible dict: {"indices": [...], "values": [...]}
    """
    model = _get_model()
    sparse = list(model.embed(text))[0]
    return {
        "indices": sparse.indices.tolist(),
        "values": sparse.values.tolist(),
    }
