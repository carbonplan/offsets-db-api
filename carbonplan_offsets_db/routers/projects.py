import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import or_
from sqlmodel import Session

from ..database import get_session
from ..logging import get_logger
from ..models import (
    Project,
    ProjectReadDetails,
    ProjectStats,
    ProjectStatsWithPagination,
    ProjectWithPagination,
)
from ..query_helpers import apply_filters, apply_sorting, handle_pagination
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

    # Filters applying 'ilike' operation
    filterable_attributes = [
        ('registry', registry, 'ilike'),
        ('country', country, 'ilike'),
    ]

    for attribute, values, operation in filterable_attributes:
        query = apply_filters(
            query=query, model=Project, attribute=attribute, values=values, operation=operation
        )

    list_attributes = [('protocol', protocol, 'ANY'), ('category', category, 'ANY')]
    for attribute, values, operation in list_attributes:
        query = apply_filters(
            query=query, model=Project, attribute=attribute, values=values, operation=operation
        )

    # Filters applying '==', '>=', or '<=' operations
    other_filters = [
        ('is_arb', is_arb, '=='),
        ('registered_at', registered_at_from, '>='),
        ('registered_at', registered_at_to, '<='),
        ('started_at', started_at_from, '>='),
        ('started_at', started_at_to, '<='),
        ('issued', issued_min, '>='),
        ('issued', issued_max, '<='),
        ('retired', retired_min, '>='),
        ('retired', retired_max, '<='),
    ]

    for attribute, values, operation in other_filters:
        query = apply_filters(
            query=query, model=Project, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
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
    response_model=ProjectStatsWithPagination,
    summary='Get aggregated statistics for all projects',
)
def get_project_stats(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    date_from: datetime.date | None = Query(default=None, description='Format: YYYY-MM-DD'),
    date_to: datetime.date | None = Query(default=None, description='Format: YYYY-MM-DD'),
    sort: list[str] = Query(
        default=['registry'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
):
    """
    Returns a list of ProjectStats objects containing aggregated statistics for all projects in the database.
    """
    logger.info('Getting projects stats')

    query = session.query(ProjectStats)

    # Filters applying 'ilike' operation
    filterable_attributes = [
        ('registry', registry, 'ilike'),
    ]

    for attribute, values, operation in filterable_attributes:
        query = apply_filters(
            query=query, model=ProjectStats, attribute=attribute, values=values, operation=operation
        )

    # Filters applying '>=', or '<=' operations
    other_filters = [('date', date_from, '>='), ('date', date_to, '<=')]

    for attribute, values, operation in other_filters:
        query = apply_filters(
            query=query, model=ProjectStats, attribute=attribute, values=values, operation=operation
        )

    if sort:
        query = apply_sorting(query=query, sort=sort, model=ProjectStats)

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        query=query, current_page=current_page, per_page=per_page, request=request
    )

    return ProjectStatsWithPagination(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=results,
    )
