#!/usr/bin/env bash
# Pixi activation script — sets local-dev defaults.
# Each variable is only exported if not already set in the environment,
# so shell exports and direnv always take precedence.

# Database URL — matches docker-compose.yml service credentials.
export OFFSETS_DB_DATABASE_URL="${OFFSETS_DB_DATABASE_URL:-postgresql://offsets_db:offsets_db@localhost:5432/offsets_db}"

# API key used for protected endpoints locally.
export OFFSETS_DB_API_KEY="${OFFSETS_DB_API_KEY:-local-dev-key}"

# Staging mode on by default locally.
export OFFSETS_DB_STAGING="${OFFSETS_DB_STAGING:-true}"

# Single worker for local dev.
export OFFSETS_DB_WEB_CONCURRENCY="${OFFSETS_DB_WEB_CONCURRENCY:-1}"
