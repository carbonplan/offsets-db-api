web: gunicorn -w 8 -t 120 -k uvicorn.workers.UvicornWorker offsets_db_api.main:app --config gunicorn_config.py --access-logfile '-' --error-logfile '-'
