#!/usr/bin/env bash

set -e


echo "Python Location: $(which python)"
echo "Python Version: $(python --version)"

echo "List of Python packages:"
python -m pip list

echo "alembic version: $(alembic --version)"

# Run database migrations
#TODO: disable alembic migrations for now to avoid breaking the build
echo "Running database migrations..."
python -m alembic upgrade head

echo "release complete!"
