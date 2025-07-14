#!/bin/bash
exec gunicorn -w "${OFFSETS_DB_WEB_CONCURRENCY:-2}" -t 600 -k uvicorn.workers.UvicornWorker offsets_db_api.main:app --config gunicorn_config.py --access-logfile '-' --error-logfile '-'
