#!/usr/bin/env bash

set -e

# Run database migrations
echo "Running database migrations..."

python -m alembic upgrade head

echo "release complete!"
