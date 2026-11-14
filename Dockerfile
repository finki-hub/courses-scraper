FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

COPY app ./app

FROM python:3.13-slim-bookworm AS final

ARG APP_REVISION=""
ENV APP_REVISION=${APP_REVISION} \
    PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=builder /app /app

# Checkpoints and in-flight CSVs; mount a volume to resume an interrupted run.
VOLUME ["/app/output"]

# The unattended FINKI Hub data refresh; override with
# `python -m app ...` to run the scraper directly.
CMD ["python", "-m", "app.sync"]
