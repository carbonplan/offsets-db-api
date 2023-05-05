from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .app_metadata import metadata
from .logging import get_logger
from .routers import health, projects

logger = get_logger()


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

    return application


app = create_application()


@app.on_event('startup')
async def startup_event():
    logger.info('Application startup...')


@app.on_event('shutdown')
async def shutdown_event():
    logger.info('Application shutdown...')
