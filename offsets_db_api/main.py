import asyncio
import os
import pathlib
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from offsets_db_api.app_metadata import metadata
from offsets_db_api.cache import clear_cache, request_key_builder, watch_dog_dir, watch_dog_file
from offsets_db_api.log import get_logger
from offsets_db_api.routers import charts, clips, credits, files, health, projects
from offsets_db_api.settings import get_settings

logger = get_logger()


class CacheInvalidationHandler(FileSystemEventHandler):
    def on_modified(self, event):
        event_path = pathlib.Path(event.src_path).resolve()
        if event_path == watch_dog_file.resolve():
            logger.info('🔄 File modified: %s', event_path)
            asyncio.run(clear_cache())


@asynccontextmanager
async def lifespan_event(app: FastAPI):
    """
    Context manager that yields the application's startup and shutdown events.
    """
    logger.info('⏱️ Application startup...')

    worker_num = int(os.environ.get('APP_WORKER_ID', 9999))

    logger.info(f'👷 Worker num: {worker_num}')

    # set up cache
    logger.info('🔥 Setting up cache...')
    expiration = int(60 * 60 * 2)  # 2 hours
    cache_status_header = 'X-OffsetsDB-Cache'
    FastAPICache.init(
        InMemoryBackend(),
        expire=expiration,
        key_builder=request_key_builder,
        cache_status_header=cache_status_header,
    )
    logger.info(
        f'🔥 Cache set up with expiration={expiration:,} seconds | {cache_status_header} cache status header.'
    )

    event_handler = CacheInvalidationHandler()
    observer = Observer()
    observer.schedule(event_handler, path=str(watch_dog_dir), recursive=False)
    observer.start()

    yield

    logger.info('Application shutdown...')
    logger.info('🧹 Clearing cache...')
    FastAPICache.reset()
    observer.stop()
    observer.join()
    logger.info('👋 Goodbye!')


def create_application() -> FastAPI:
    application = FastAPI(**metadata, lifespan=lifespan_event)
    settings = get_settings()
    application.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in settings.allowed_origins.split(',')],
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
