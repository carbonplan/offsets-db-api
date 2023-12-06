#!/usr/bin/env bash

set -e

echo "List of Python packages:"
python -m pip list

echo "alembic version: $(python -m alembic --version)"

# Run database migrations
echo "Running database migrations..."

echo "testing Python: $(python -c "print('Hello World')")"
python -m alembic upgrade head

echo "release complete!"
