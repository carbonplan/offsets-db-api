import datetime
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi_cache.decorator import cache
from sqlalchemy import or_
from sqlalchemy.orm import contains_eager
from sqlmodel import Session, col, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.database import get_session
from offsets_db_api.logging import get_logger
from offsets_db_api.models import Clip, ClipProject, PaginatedProjects, Project, ProjectWithClips
from offsets_db_api.schemas import Pagination, Registries
from offsets_db_api.security import check_api_key
from offsets_db_api.sql_helpers import apply_filters, apply_sorting, handle_pagination

router = APIRouter()
logger = get_logger()


@router.get('/', response_model=PaginatedProjects)
@cache(namespace=CACHE_NAMESPACE)
async def get_projects(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
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

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('protocol', protocol, 'ALL', Project),
        ('category', category, 'ALL', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('listed_at', listed_at_from, '>=', Project),
        ('listed_at', listed_at_to, '<=', Project),
        ('issued', issued_min, '>=', Project),
        ('issued', issued_max, '<=', Project),
        ('retired', retired_min, '>=', Project),
        ('retired', retired_max, '<=', Project),
    ]
    statement = (
        select(Project, Clip)
        .join(ClipProject, col(ClipProject.project_id) == col(Project.project_id))
        .join(Clip, col(Clip.id) == col(ClipProject.clip_id))
        .options(contains_eager(Project.clip_relationships).contains_eager(ClipProject.clip))
    )

    for attribute, values, operation, model in filters:
        statement = apply_filters(
            statement=statement,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    if search:
        search_pattern = f'%{search}%'
        statement = statement.where(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
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

    # Execute the query
    project_clip_pairs = results

    # Group clips by project using a dictionary and project_id as the key
    project_to_clips = defaultdict(list)
    projects = {}
    for project, clip in project_clip_pairs:
        p_id = project.project_id
        if p_id not in projects:
            projects[p_id] = project
        project_to_clips[p_id].append(clip)

    # Transform the dictionary into a list of projects with clips
    projects_with_clips = []
    for project_id, clips in project_to_clips.items():
        project = projects[project_id]
        project_data = project.model_dump()
        project_data['clips'] = [clip.model_dump() for clip in clips if clip is not None]
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

    # Start the query to get the project and related clips
    statement = (
        select(Project)
        .join(Project.clip_relationships, isouter=True)
        .join(col(ClipProject.clip), isouter=True)
        .options(contains_eager(Project.clip_relationships).contains_eager(ClipProject.clip))
        .where(col(Project.project_id) == project_id)
    )

    if not (project_with_clips := session.exec(statement).unique().one_or_none()):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f'project {project_id} not found'
        )
    # Extract the Project and related Clips from the query result
    project = project_with_clips
    project_data = project.model_dump()
    project_data['clips'] = [
        clip_project.clip.model_dump()
        for clip_project in project.clip_relationships
        if clip_project.clip
    ]
    return project_data
