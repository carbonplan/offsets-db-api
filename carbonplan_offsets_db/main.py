from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .app_metadata import metadata
from .logging import get_logger
from .routers import credits, files, health, projects
from .tasks import calculate_totals, update_credit_stats, update_project_stats

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
    logger.info('Application startup...')
    scheduler.add_job(calculate_totals)
    scheduler.add_job(update_project_stats)
    scheduler.add_job(update_credit_stats)
    scheduler.add_job(calculate_totals, 'interval', hours=12)
    scheduler.start()


@app.on_event('shutdown')
async def shutdown_event():
    logger.info('Application shutdown...')
    scheduler.shutdown()
