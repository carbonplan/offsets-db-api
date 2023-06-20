import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import or_
from sqlmodel import Session

from ..database import get_session
from ..logging import get_logger
from ..models import Project, ProjectReadDetails, ProjectStats, ProjectWithPagination
from ..query_helpers import apply_sorting, handle_pagination
from ..schemas import Pagination, Registries

router = APIRouter()
logger = get_logger()


@router.get('/', response_model=ProjectWithPagination)
def get_projects(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_arb: bool | None = Query(None, description='Whether project is an ARB project'),
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
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
    search: str
    | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    sort: list[str] = Query(
        default=['project_id'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    session: Session = Depends(get_session),
):
    """Get projects with pagination and filtering"""
    logger.info(f'Getting projects: {request.url}')

    query = session.query(Project)

    if registry:
        query = query.filter(or_(*[Project.registry.ilike(r) for r in registry]))

    if country:
        query = query.filter(or_(*[Project.country.ilike(c) for c in country]))

    if protocol:
        query = query.filter(or_(*[Project.protocol.ilike(p) for p in protocol]))

    if category:
        query = query.filter(or_(*[Project.category.ilike(c) for c in category]))

    if is_arb is not None:
        query = query.filter(Project.is_arb == is_arb)

    if registered_at_from:
        query = query.filter(Project.registered_at >= registered_at_from)

    if registered_at_to:
        query = query.filter(Project.registered_at <= registered_at_to)

    if started_at_from:
        query = query.filter(Project.started_at >= started_at_from)

    if started_at_to:
        query = query.filter(Project.started_at <= started_at_to)

    # Add additional filters for issued and retired credits
    if issued_min:
        query = query.filter(Project.issued >= issued_min)

    if issued_max:
        query = query.filter(Project.issued <= issued_max)

    if retired_min:
        query = query.filter(Project.retired >= retired_min)

    if retired_max:
        query = query.filter(Project.retired <= retired_max)

    if search:
        search_pattern = (
            f'%{search}%'  # Wrapping search string with % to match anywhere in the string
        )
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    if sort:
        query = apply_sorting(query=query, sort=sort, model=Project)

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        query=query, current_page=current_page, per_page=per_page, request=request
    )

    return ProjectWithPagination(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=results,
    )


@router.get(
    '/{project_id}',
    response_model=ProjectReadDetails,
    summary='Get project details by project_id',
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


@router.get(
    '/stats/',
    response_model=list[ProjectStats],
    summary='Get aggregated statistics for all projects',
)
def get_project_stats(
    session: Session = Depends(get_session),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    date_from: datetime.date | None = Query(default=None, description='Format: YYYY-MM-DD'),
    date_to: datetime.date | None = Query(default=None, description='Format: YYYY-MM-DD'),
    sort: list[str] = Query(
        default=['registry'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
):
    """
    Returns a list of ProjectStats objects containing aggregated statistics for all projects in the database.
    """
    logger.info('Getting projects stats')

    query = session.query(ProjectStats)

    if registry:
        query = query.filter(or_(*[ProjectStats.registry.ilike(r) for r in registry]))

    if date_from:
        query = query.filter(ProjectStats.date >= date_from)

    if date_to:
        query = query.filter(ProjectStats.date <= date_to)

    if sort:
        query = apply_sorting(query=query, sort=sort, model=ProjectStats)

    return query.all()
