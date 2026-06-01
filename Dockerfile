# Build stage using pixi
FROM ubuntu:24.04 AS build

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install git, ca-certificates, and curl (needed for pixi installer and git+https:// deps)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Install pixi — pin to the same version used to generate pixi.lock to avoid
# lock-file-not-up-to-date failures when the ghcr.io/prefix-dev/pixi:latest image lags behind.
RUN curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION=v0.70.0 bash
ENV PATH="/root/.pixi/bin:$PATH"

# Copy source code, pixi.toml and pixi.lock to the container
WORKDIR /app
COPY . .

# Install dependencies using pixi (uses the default environment)
RUN pixi install --locked

# Create the shell-hook bash script to activate the environment
RUN pixi shell-hook > /shell-hook.sh

# Extend the shell-hook script to run the command passed to the container
RUN echo 'exec "$@"' >> /shell-hook.sh

# Production stage
FROM ubuntu:24.04 AS production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install runtime dependencies (libpq for PostgreSQL)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Copy the pixi environment from the build stage
# Note: the prefix path must stay the same as in the build container
COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=build /shell-hook.sh /shell-hook.sh
COPY --from=build /app /app
WORKDIR /app

# Expose the port
EXPOSE 8000

# Set the entrypoint to the shell-hook script (activates the environment and runs the command)
ENTRYPOINT ["/bin/bash", "/shell-hook.sh"]

# Run the application using gunicorn (supports OFFSETS_DB_WEB_CONCURRENCY and PORT env vars)
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT:-8000} -w ${OFFSETS_DB_WEB_CONCURRENCY:-2} -t 600 -k uvicorn.workers.UvicornWorker offsets_db_api.main:app --config gunicorn_config.py --access-logfile - --error-logfile -"]
