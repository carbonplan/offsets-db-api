import datetime
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi_cache.decorator import cache
from sqlalchemy import or_
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, distinct, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.database import get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import (
    Clip,
    ClipProject,
    Credit,
    PaginatedProjects,
    Project,
    ProjectType,
    ProjectWithClips,
)
from offsets_db_api.schemas import Pagination, Registries
from offsets_db_api.security import check_api_key
from offsets_db_api.sql_helpers import apply_filters, apply_sorting, handle_pagination

router = APIRouter()
logger = get_logger()


@router.get(
    '/', response_model=PaginatedProjects, summary='Get projects with pagination and filtering'
)
@cache(namespace=CACHE_NAMESPACE)
async def get_projects(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    project_type: list[str] | None = Query(None, description='Project type'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    listed_at_from: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    listed_at_to: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    beneficiary_search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on specified beneficiary_search_fields only.',
    ),
    beneficiary_search_fields: list[str] = Query(
        default=[
            'retirement_beneficiary',
            'retirement_account',
            'retirement_note',
            'retirement_reason',
        ],
        description='Beneficiary fields to search in',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    sort: list[str] = Query(
        default=['project_id'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get projects with pagination and filtering"""

    logger.info(f'Getting projects: {request.url}')

    Credit_alias = aliased(Credit)

    matching_projects = select(distinct(Project.project_id)).outerjoin(
        Credit_alias, col(Project.project_id) == col(Credit_alias.project_id)
    )

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('listed_at', listed_at_from, '>=', Project),
        ('listed_at', listed_at_to, '<=', Project),
        ('issued', issued_min, '>=', Project),
        ('issued', issued_max, '<=', Project),
        ('retired', retired_min, '>=', Project),
        ('retired', retired_max, '<=', Project),
        ('project_type', project_type, 'ilike', ProjectType),
    ]

    if search:
        search_pattern = f'%{search}%'
        matching_projects = matching_projects.where(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    if beneficiary_search:
        beneficiary_search_pattern = f'%{beneficiary_search}%'
        beneficiary_search_conditions = []

        for field in beneficiary_search_fields:
            if hasattr(Credit_alias, field):
                beneficiary_search_conditions.append(
                    getattr(Credit_alias, field).ilike(beneficiary_search_pattern)
                )
            elif hasattr(Project, field):
                beneficiary_search_conditions.append(
                    getattr(Project, field).ilike(beneficiary_search_pattern)
                )

        matching_projects = matching_projects.where(or_(*beneficiary_search_conditions))

    matching_projects_select = select(matching_projects.subquery())

    # Use the subquery to filter the main query
    statement = (
        select(Project, ProjectType.project_type, ProjectType.source)
        .outerjoin(ProjectType, col(Project.project_id) == col(ProjectType.project_id))
        .where(col(Project.project_id).in_(matching_projects_select))
    )

    for attribute, values, operation, model in filters:
        statement = apply_filters(
            statement=statement,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    if sort:
        statement = apply_sorting(
            statement=statement, sort=sort, model=Project, primary_key='project_id'
        )

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        statement=statement,
        primary_key=Project.project_id,
        current_page=current_page,
        per_page=per_page,
        request=request,
        session=session,
    )

    # Get the list of project IDs from the results
    project_ids = [project.project_id for project, _, _ in results]

    # Subquery to get clips related to the project IDs
    clip_statement = (
        select(ClipProject.project_id, Clip)
        .where(col(ClipProject.project_id).in_(project_ids))
        .join(Clip, col(Clip.id) == col(ClipProject.clip_id))
    )
    clip_results = session.exec(clip_statement).all()

    # Group clips by project_id
    project_to_clips = defaultdict(list)
    for project_id, clip in clip_results:
        project_to_clips[project_id].append(clip)

    # Transform the dictionary into a list of projects with clips and project_type
    projects_with_clips = []
    for project, project_type, project_type_source in results:
        project_data = project.model_dump()
        project_data['project_type'] = project_type
        project_data['project_type_source'] = project_type_source
        project_data['clips'] = [
            clip.model_dump() for clip in project_to_clips.get(project.project_id, [])
        ]
        projects_with_clips.append(project_data)

    return PaginatedProjects(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=projects_with_clips,
    )


@router.get(
    '/{project_id}',
    response_model=ProjectWithClips,
    summary='Get project details by project_id',
)
@cache(namespace=CACHE_NAMESPACE)
async def get_project(
    request: Request,
    project_id: str,
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get a project by registry and project_id"""
    logger.info(f'Getting project: {request.url}')

    # main query to get the project details
    statement = (
        select(Project, ProjectType.project_type, ProjectType.source)
        .outerjoin(ProjectType, Project.project_id == ProjectType.project_id)
        .where(Project.project_id == project_id)
    )

    result = session.exec(statement).one_or_none()

    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f'project {project_id} not found'
        )

    project, project_type, project_type_source = result

    # Subquery to get the related clips
    clip_statement = (
        select(Clip)
        .join(ClipProject, col(Clip.id) == col(ClipProject.clip_id))
        .where(ClipProject.project_id == project_id)
    )
    clip_projects_subquery = session.exec(clip_statement).all()

    # Construct the response data
    project_data = project.model_dump()
    project_data['project_type'] = project_type
    project_data['project_type_source'] = project_type_source
    project_data['clips'] = [clip.model_dump() for clip in clip_projects_subquery]
    return project_data
