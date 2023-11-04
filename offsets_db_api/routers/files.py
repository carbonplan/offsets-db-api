import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlmodel import Session

from ..database import get_engine, get_session
from ..logging import get_logger
from ..models import File, FileCategory, FileStatus
from ..schemas import FileURLPayload
from ..settings import get_settings
from ..tasks import process_files

router = APIRouter()
logger = get_logger()


@router.post(
    '/',
    response_model=list[File],
    summary='Submit a file to be processed and added to the database',
)
def submit_file(
    payload: list[FileURLPayload],
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session),
):
    """Submit a file to the database"""
    logger.info('Received file(s) %s', payload)

    file_objs = []
    for p in payload:
        file_obj = File(
            url=p.url,
            category=p.category,
        )
        file_objs.append(file_obj)
        session.add(file_obj)

    session.commit()

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)

    background_tasks.add_task(process_files, engine=engine, session=session, files=file_objs)
    return file_objs


@router.get('/{file_id}', response_model=File, summary='Get a file by id')
def get_file(file_id: int, session: Session = Depends(get_session)):
    """Get a file by id"""
    logger.info('Getting file %s', file_id)

    if file_obj := session.query(File).get(file_id):
        return file_obj
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f'file {file_id} not found',
        )


@router.get('/', response_model=list[File], summary='List files')
def get_files(
    category: FileCategory | None = None,
    status: FileStatus | None = None,
    recorded_at_from: datetime.date | datetime.datetime | None = None,
    recorded_at_to: datetime.date | datetime.datetime | None = None,
    limit: int = 100,
    offset: int = 0,
    session: Session = Depends(get_session),
):
    """Get files"""
    logger.info(
        'Getting files with filter: category=%s, status=%s, recorded_at_from=%s, recorded_at_to=%s, limit=%d, offset=%d',
        category,
        status,
        recorded_at_from,
        recorded_at_to,
        limit,
        offset,
    )

    query = session.query(File)

    if category:
        query = query.filter_by(category=category)

    if status:
        query = query.filter_by(status=status)

    if recorded_at_from:
        query = query.filter(File.recorded_at >= recorded_at_from)

    if recorded_at_to:
        query = query.filter(File.recorded_at <= recorded_at_to)

    files = query.limit(limit).offset(offset).all()

    logger.info('Found %d files', len(files))

    return files
