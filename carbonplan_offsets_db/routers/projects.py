import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlmodel import Session

from ..database import get_session
from ..logging import get_logger
from ..models import File, FileCategory, FileStatus, Project, ProjectRead, ProjectReadDetails
from ..query_helpers import apply_sorting
from ..schemas import FileURLPayload, Registries
from ..tasks import process_files

router = APIRouter()
logger = get_logger()


@router.post(
    '/files',
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
        file_obj = File(url=p.url, category='projects')
        file_objs.append(file_obj)
        session.add(file_obj)

    session.commit()

    background_tasks.add_task(process_files, session=session, files=file_objs)
    return file_objs


@router.get('/files/{file_id}', response_model=File, summary='Get a file by id')
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


@router.get('/files', response_model=list[File], summary='List files')
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


@router.get('/', response_model=list[ProjectRead])
def get_projects(
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    registered_at_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    registered_at_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    started_at_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    started_at_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    search: str
    | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    limit: int = Query(50, description='Limit number of results', le=100, gt=0),
    offset: int = Query(0, description='Offset results', ge=0),
    sort: list[str] = Query(
        default=['project_id'],
        description='List of sorting parameters in the format "field_name" for ascending order or "-field_name" for descending order.',
    ),
    session: Session = Depends(get_session),
):
    """Get projects with pagination and filtering"""
    logger.info(
        'Getting projects with filter: registry=%s, country=%s, protocol=%s, search=%s, limit=%d, offset=%d',
        registry,
        country,
        protocol,
        search,
        limit,
        offset,
    )

    query = session.query(Project)

    if registry:
        query = query.filter(or_(*[Project.registry.ilike(r) for r in registry]))

    if country:
        query = query.filter(or_(*[Project.country.ilike(c) for c in country]))

    if protocol:
        query = query.filter(or_(*[Project.protocol.ilike(p) for p in protocol]))

    if registered_at_from:
        query = query.filter(Project.registered_at >= registered_at_from)

    if registered_at_to:
        query = query.filter(Project.registered_at <= registered_at_to)

    if started_at_from:
        query = query.filter(Project.started_at >= started_at_from)

    if started_at_to:
        query = query.filter(Project.started_at <= started_at_to)

    if search:
        search_pattern = (
            f'%{search}%'  # Wrapping search string with % to match anywhere in the string
        )
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    if sort:
        query = apply_sorting(query=query, sort=sort, model=Project)

    projects = query.limit(limit).offset(offset).all()

    logger.info('Found %d projects', len(projects))
    return projects


@router.get(
    '/{project_id}',
    response_model=ProjectReadDetails,
    summary='Get a project by registry and project_id',
)
def get_project(
    project_id: str,
    session: Session = Depends(get_session),
):
    """Get a project by registry and project_id"""
    logger.info('Getting project %s', project_id)

    if project_obj := session.query(Project).filter_by(project_id=project_id).one_or_none():
        return project_obj
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f'project {project_id} not found',
        )
