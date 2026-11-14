FROM ghcr.io/astral-sh/uv:python3.15-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev --no-build

COPY app ./app

FROM python:3.15-slim-trixie AS final

ARG APP_REVISION=""
ENV APP_REVISION=${APP_REVISION} \
    PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

RUN useradd --system --uid 10001 --home-dir /app scraper

WORKDIR /app

COPY --from=builder /app /app
RUN mkdir /app/output && chown scraper /app/output

USER scraper

# Checkpoints and in-flight CSVs; mount a volume to resume an interrupted run.
VOLUME ["/app/output"]

# The unattended FINKI Hub data refresh; override with
# `python -m app ...` to run the scraper directly.
CMD ["python", "-m", "app.sync"]
