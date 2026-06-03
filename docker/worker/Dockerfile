# Worker image — built from repo root (context: ..)
# so that memos_config/ and config/ are accessible alongside docker/worker/.
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps (worker-specific)
COPY docker/worker/requirements.txt /tmp/worker-requirements.txt
RUN pip install --no-cache-dir -r /tmp/worker-requirements.txt

# Worker source
COPY docker/worker/ /app/

# Shared config + loader (single source of truth for hosts/models)
COPY memos_config/ /app/memos_config/
COPY config/       /app/config/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CONFIG_PATH=/app/config/services.yaml

RUN useradd -r -u 10001 appuser
USER appuser

CMD ["python", "main.py", "--run-worker"]
