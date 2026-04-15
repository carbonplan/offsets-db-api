# Build stage using pixi
FROM ghcr.io/prefix-dev/pixi:0.67.0 AS build

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install git and ca-certificates (needed for git+https:// dependencies in pyproject.toml)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy source code, pyproject.toml and pixi.lock to the container
# (.pixi/ is excluded via .dockerignore so pixi always installs from scratch)
WORKDIR /app
COPY . .

# Install dependencies using pixi (uses the default environment)
RUN pixi install --locked

# Create the shell-hook bash script to activate the environment
RUN pixi shell-hook --shell bash > /shell-hook.sh

# Extend the shell-hook script to run the command passed to the container
RUN echo 'exec "$@"' >> /shell-hook.sh

# Production stage
FROM ubuntu:24.04 AS production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Belt-and-suspenders: ensure pixi env binaries are on PATH
ENV PATH="/app/.pixi/envs/default/bin:$PATH"

# Install runtime dependencies (libpq for PostgreSQL)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Copy only the pixi environment and shell-hook from the build stage
# (per https://github.com/prefix-dev/pixi-docker — prefix path must stay the same)
COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=build /shell-hook.sh /shell-hook.sh

# Copy application source code from build stage
# (needed because offsets-db-api is an editable install pointing to /app)
COPY --from=build /app/offsets_db_api /app/offsets_db_api
COPY --from=build /app/gunicorn_config.py /app/gunicorn_config.py
COPY --from=build /app/migrations /app/migrations
COPY --from=build /app/alembic.ini /app/alembic.ini
COPY --from=build /app/release.sh /app/release.sh
COPY --from=build /app/scripts /app/scripts
COPY --from=build /app/pyproject.toml /app/pyproject.toml

WORKDIR /app

# Expose the port
EXPOSE 8000

# Set the entrypoint to the shell-hook script (activates the environment and runs the command)
ENTRYPOINT ["/bin/bash", "/shell-hook.sh"]

# Run the application using gunicorn (supports OFFSETS_DB_WEB_CONCURRENCY and PORT env vars)
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT:-8000} -w ${OFFSETS_DB_WEB_CONCURRENCY:-2} -t 600 -k uvicorn.workers.UvicornWorker offsets_db_api.main:app --config gunicorn_config.py --access-logfile - --error-logfile -"]
