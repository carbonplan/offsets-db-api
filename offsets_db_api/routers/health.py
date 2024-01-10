from collections import defaultdict

from fastapi import APIRouter, Depends
from sqlmodel import Session, col, select

from ..database import get_session
from ..logging import get_logger
from ..models import File, FileCategory, FileStatus
from ..security import check_api_key
from ..settings import Settings, get_settings

router = APIRouter()
logger = get_logger()


@router.get('/')
def status(settings: Settings = Depends(get_settings), session: Session = Depends(get_session)):
    logger.info('Received status request')
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

    db_latest_update = {}
    for category, entries in grouped_files.items():
        db_latest_update[category] = {
            'date': entries[0]['date'],
            'url': entries[0]['url'],
        }

    return {
        'status': 'ok',
        'staging': settings.staging,
        'latest-successful-db-update': db_latest_update,
    }


@router.get('/authorized_user')
def validate_authorized_user(authorized_user: bool = Depends(check_api_key)):
    return {'authorized_user': authorized_user}
