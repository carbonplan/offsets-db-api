import os

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .app_metadata import metadata
from .logging import get_logger
from .models import Credit, Project
from .routers import credits, files, health, projects
from .settings import get_settings
from .tasks import calculate_totals, export_table_to_csv, update_credit_stats, update_project_stats

logger = get_logger()

# Initialize scheduler
scheduler = BackgroundScheduler()


def create_application() -> FastAPI:
    application = FastAPI(**metadata)
    # TODO: figure out how to set origins to only the frontend domain
    # in the meantime, we can allow everything.
    origins = ['*']  # is this dangerous? I don't think so, but I'm not sure.
    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    application.include_router(health.router, prefix='/health', tags=['health'])
    application.include_router(projects.router, prefix='/projects', tags=['projects'])
    application.include_router(credits.router, prefix='/credits', tags=['credits'])
    application.include_router(files.router, prefix='/files', tags=['files'])

    return application


app = create_application()


@app.on_event('startup')
async def startup_event():
    """
    Event handler for application startup.
    If the current worker is the first one, it starts the scheduler.
    """
    logger.info('⏱️ Application startup...')

    worker_num = int(os.environ.get('APP_WORKER_ID', 9999))

    logger.info(f'👷 Worker num: {worker_num}')

    # Only run scheduled jobs in the first worker
    if worker_num not in [1, 9999]:
        logger.info(f'👷 Worker {worker_num} is not the first worker, not starting scheduler.')
        return

    logger.info('🚀 Starting scheduler...')
    # Add your scheduler jobs here
    scheduler.add_job(calculate_totals)
    # Remove these two lines once we have a better way to update stats
    scheduler.add_job(update_project_stats)
    scheduler.add_job(update_credit_stats)
    scheduler.add_job(calculate_totals, 'interval', hours=12)
    # run at 3am every sunday morning
    scheduler.add_job(update_project_stats, 'cron', day_of_week=0, hour=3)
    scheduler.add_job(update_credit_stats, 'cron', day_of_week=0, hour=3)

    settings = get_settings()
    scheduler.add_job(export_table_to_csv, kwargs={'table': Project, 'path': settings.export_path})
    scheduler.add_job(export_table_to_csv, kwargs={'table': Credit, 'path': settings.export_path})

    scheduler.add_job(
        export_table_to_csv,
        'cron',
        day_of_week=0,
        hour=5,
        kwargs={'table': Project, 'path': settings.export_path},
    )
    scheduler.add_job(
        export_table_to_csv,
        'cron',
        day_of_week=0,
        hour=5,
        kwargs={'table': Credit, 'path': settings.export_path},
    )
    scheduler.start()


@app.on_event('shutdown')
async def shutdown_event():
    logger.info('Application shutdown...')
    scheduler.shutdown()
