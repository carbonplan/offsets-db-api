import asyncio
import os
import pathlib
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from starlette.middleware.base import BaseHTTPMiddleware
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from offsets_db_api.app_metadata import metadata
from offsets_db_api.cache import clear_cache, request_key_builder, watch_dog_dir, watch_dog_file
from offsets_db_api.log import get_logger
from offsets_db_api.routers import charts, clips, credits, files, health, projects

logger = get_logger()


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log request details including client IP, method, path, and response time."""

    async def dispatch(self, request: Request, call_next):
        # Get the real client IP from proxy headers
        client_ip = (
            request.headers.get('fly-client-ip')
            or request.headers.get('x-forwarded-for', '').split(',')[0].strip()
            or request.headers.get('x-real-ip')
            or request.client.host
            if request.client
            else 'unknown'
        )

        # Get request details
        method = request.method
        path = request.url.path
        query = request.url.query
        user_agent = request.headers.get('user-agent', 'unknown')

        # Start timing
        start_time = time.time()

        # Process request
        response = await call_next(request)

        # Calculate response time
        process_time = (time.time() - start_time) * 1000  # Convert to milliseconds

        # Log the request
        full_path = f'{path}?{query}' if query else path
        logger.info(
            f'request details: {client_ip} - "{method} {full_path}" {response.status_code} {process_time:.2f}ms - {user_agent}'
        )

        return response


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

    # Add request logging middleware
    application.add_middleware(RequestLoggingMiddleware)

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
