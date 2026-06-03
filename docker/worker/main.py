"""
Cognitive Worker ARQ — Memory-as-a-Service (MaaS)
Main entry point for the processing worker. Reads service configuration from
config/services.yaml via the memos_config loader.
"""
import logging
import sys
from pathlib import Path

# ─── Make memos_config importable (container: /app, dev: repo root) ─────────
_here = Path(__file__).resolve()
for _candidate in (_here.parent, *_here.parents):
    if (_candidate / "memos_config" / "loader.py").exists():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

from memos_config import config  # noqa: E402

# ─── Logging configuration ──────────────────────────────────────────────────
LOG_LEVEL = config.logging.level if hasattr(config, "logging") else "INFO"
logging.basicConfig(
    level=getattr(logging, str(LOG_LEVEL).upper()),
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cognitive-worker")

# ─── ARQ + Redis (Valkey) ───────────────────────────────────────────────────
from arq import cron  # noqa: E402
from arq.connections import RedisSettings  # noqa: E402

from tasks.ingestion import ingest_memory  # noqa: E402
from tasks.reflection import reflect_on_memories, micro_reflection  # noqa: E402
from tasks.file_ingestion import ingest_file  # noqa: E402
from services.db import get_pool, close_pool  # noqa: E402

valkey = config.valkey
redis_settings = RedisSettings(
    host=valkey.host,
    port=int(valkey.port),
    password=(valkey.password or None),
    database=int(valkey.db),
)


# ─── Startup/shutdown functions ────────────────────────────────────────────
async def startup(ctx):
    """Connect to Qdrant + Postgres pool. Validate the Qdrant collection."""
    from services.local_qdrant import get_qdrant_client, ensure_collection

    logger.info("Worker starting...")
    ctx["qdrant"] = get_qdrant_client()
    await ensure_collection(ctx["qdrant"])
    logger.info("Qdrant connection validated")

    ctx["pg"] = await get_pool()
    logger.info("Postgres pool ready")


async def shutdown(ctx):
    """Clean up connections."""
    logger.info("Worker shutting down...")
    if "qdrant" in ctx:
        await ctx["qdrant"].close()
    await close_pool()


# ─── ARQ function definitions ────────────────────────────────────────────
async def process_ingestion(ctx, memory_text: str, source: str, tags: list = None):
    """ARQ job: ingest a memory into the vector store."""
    return await ingest_memory(ctx["qdrant"], memory_text, source, tags)


async def process_wiki_file(ctx, file_path: str):
    """ARQ job: ingest a .md file from the vault."""
    return await ingest_file(ctx["qdrant"], file_path)


async def process_reflection(ctx):
    """ARQ job: run periodic reflection."""
    return await reflect_on_memories(ctx["qdrant"])


async def process_micro_reflection(ctx):
    """ARQ job: run on-demand micro-reflection (Phase 3)."""
    return await micro_reflection(ctx["qdrant"])


# ─── ARQ Worker Settings ─────────────────────────────────────────────────
class WorkerSettings:
    """ARQ worker settings."""
    redis_settings = redis_settings
    functions = [process_ingestion, process_reflection, process_micro_reflection, process_wiki_file]
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = int(config.valkey.arq.max_jobs)
    job_timeout = int(config.valkey.arq.job_timeout)
    keep_result = int(config.valkey.arq.keep_result)
    cron_jobs = [
        # Reflection every 2 hours (minute 0 of even hours)
        cron(process_reflection, hour={0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22}, minute=0),
        # Micro-reflection now triggered via reflection_trigger.py (idle + budget)
    ]


# ─── Entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--run-worker":
        import subprocess
        logger.info("Starting ARQ worker...")
        subprocess.run(["arq", "main.WorkerSettings"])
    else:
        print("Usage: python main.py --run-worker")
        print("")
        print("To enqueue jobs, use the enqueue_host.py script")
