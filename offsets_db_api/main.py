import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .app_metadata import metadata
from .logging import get_logger
from .routers import charts, clips, credits, files, health, projects

logger = get_logger()


@asynccontextmanager
async def lifespan_event(app: FastAPI):
    """
    Context manager that yields the application's startup and shutdown events.
    """
    logger.info('⏱️ Application startup...')

    worker_num = int(os.environ.get('APP_WORKER_ID', 9999))

    logger.info(f'👷 Worker num: {worker_num}')

    yield

    logger.info('Application shutdown...')
    logger.info('👋 Goodbye!')


def create_application() -> FastAPI:
    application = FastAPI(**metadata, lifespan=lifespan_event)
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
    application.include_router(charts.router, prefix='/charts', tags=['charts'])
    application.include_router(clips.router, prefix='/clips', tags=['clips'])
    application.include_router(files.router, prefix='/files', tags=['files'])

    return application


app = create_application()
