import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status
from fastapi_cache.decorator import cache
from sqlmodel import Session, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.database import get_engine, get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import File, FileCategory, FileStatus, PaginatedFiles, Pagination
from offsets_db_api.schemas import FileURLPayload
from offsets_db_api.security import check_api_key
from offsets_db_api.settings import get_settings
from offsets_db_api.sql_helpers import apply_filters, apply_sorting, handle_pagination
from offsets_db_api.tasks import process_files

router = APIRouter()
logger = get_logger()


@router.post(
    '/',
    response_model=list[File],
    summary='Submit a file to be processed and added to the database',
)
async def submit_file(
    payload: list[FileURLPayload],
    background_tasks: BackgroundTasks,
    chunk_size: int = Query(5000, description='Chunk size for processing'),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Submit a file to the database"""
    logger.info('Received file(s) %s', payload)

    file_objs = []
    for p in payload:
        file_obj = File(url=p.url, category=p.category)
        file_objs.append(file_obj)
        session.add(file_obj)

    session.commit()
    for file_obj in file_objs:
        session.refresh(file_obj)

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)

    logger.info(f'Processing files: {file_objs}')

    background_tasks.add_task(
        process_files, engine=engine, session=session, files=file_objs, chunk_size=chunk_size
    )
    return file_objs


@router.get('/{file_id}', response_model=File, summary='Get a file by id')
@cache(namespace=CACHE_NAMESPACE)
async def get_file(
    file_id: int,
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get a file by id"""
    logger.info('Getting file %s', file_id)

    statement = select(File).where(File.id == file_id)
    if file_obj := session.exec(statement).one_or_none():
        return file_obj
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f'File {file_id} not found',
        )


@router.get('/', response_model=PaginatedFiles, summary='List files')
@cache(namespace=CACHE_NAMESPACE)
async def get_files(
    request: Request,
    category: FileCategory | None = None,
    status: FileStatus | None = None,
    recorded_at_from: datetime.datetime | None = None,
    recorded_at_to: datetime.datetime | None = None,
    sort: list[str] = Query(
        default=['recorded_at'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get files"""
    logger.info(f'Getting files with filter: {request.url}')

    filters = [
        ('category', category, '==', File),
        ('status', status, '==', File),
        ('recorded_at', recorded_at_from, '>=', File),
        ('recorded_at', recorded_at_to, '<=', File),
    ]

    statement = select(File)
    for attribute, values, operation, model in filters:
        statement = apply_filters(
            statement=statement,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    if sort:
        statement = apply_sorting(statement=statement, sort=sort, model=File, primary_key='id')

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        statement=statement,
        primary_key=File.id,
        current_page=current_page,
        per_page=per_page,
        request=request,
        session=session,
    )

    return PaginatedFiles(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=results,
    )
