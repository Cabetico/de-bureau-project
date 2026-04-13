# ingestion/Dockerfile
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock* ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY ingestar_circulo_credito.py .
COPY producer_cc.py .

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT []
CMD ["sleep", "infinity"]

# ENTRYPOINT ["python"]
# CMD ["ingestar_circulo_credito.py"]
