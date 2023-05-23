import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlmodel import Session

from ..database import get_session
from ..logging import get_logger
from ..models import Project, ProjectRead, ProjectReadDetails
from ..query_helpers import apply_sorting
from ..schemas import Registries

router = APIRouter()
logger = get_logger()


@router.get('/', response_model=list[ProjectRead])
def get_projects(
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
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
    limit: int = Query(500, description='Limit number of results', le=1000, gt=0),
    offset: int = Query(0, description='Offset results', ge=0),
    sort: list[str] = Query(
        default=['project_id'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    session: Session = Depends(get_session),
):
    """Get projects with pagination and filtering"""
    logger.info(
        'Getting projects with filter: registry=%s, country=%s, protocol=%s, category=%s, search=%s, limit=%d, offset=%d',
        registry,
        country,
        protocol,
        category,
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

    if category:
        query = query.filter(or_(*[Project.category.ilike(c) for c in category]))

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
