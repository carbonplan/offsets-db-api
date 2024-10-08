import typing
from collections import defaultdict

from fastapi import APIRouter, Depends, Request
from fastapi_cache.decorator import cache
from sqlmodel import Session, col, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.database import get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import File, FileCategory, FileStatus
from offsets_db_api.security import check_api_key
from offsets_db_api.settings import Settings, get_settings

router = APIRouter()
logger = get_logger()


@router.get('/')
async def status(settings: Settings = Depends(get_settings)) -> dict[str, typing.Any]:
    logger.info('Received status request')
    return {'status': 'ok', 'staging': settings.staging}


@router.get('/database')
@cache(namespace=CACHE_NAMESPACE, expire=60)
async def db_status(
    request: Request,
    settings: Settings = Depends(get_settings),
    session: Session = Depends(get_session),
) -> dict[str, typing.Any]:
    """Returns the latest successful db update for each file category."""
    logger.info(f'Received status request: {request.url}')
    statement = (
        select(File.category, File.recorded_at, File.url)
        .where(
            (File.status == FileStatus.success)
            & (
                (File.category == FileCategory.projects)
                | (File.category == FileCategory.credits)
                | (File.category == FileCategory.clips)
            )
        )
        .order_by(col(File.recorded_at).desc())
    )
    results = session.exec(statement).all()
    # Group by category and collect recorded_at dates
    grouped_files = defaultdict(list)
    for category, recorded_at, url in results:
        grouped_files[category.value].append(
            {'date': recorded_at.strftime('%a, %b %d %Y %H:%M:%S UTC'), 'url': url}
        )

    db_latest_update = {
        category: {
            'date': entries[0]['date'],
            'url': entries[0]['url'],
        }
        for category, entries in grouped_files.items()
    }
    return {
        'status': 'ok',
        'staging': settings.staging,
        'database-pool-size': settings.database_pool_size,
        'web-concurrency': settings.web_concurrency,
        'latest-successful-db-update': db_latest_update,
    }


@router.get('/authorized_user')
async def validate_authorized_user(authorized_user: bool = Depends(check_api_key)):
    return {'authorized_user': authorized_user}
