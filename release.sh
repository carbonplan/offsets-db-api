#!/usr/bin/env bash

set -e


echo "Python Location: $(which python)"
echo "Python Version: $(python --version)"


# Run database migrations
echo "Running database migrations..."
python -m alembic upgrade head

echo "release complete!"
