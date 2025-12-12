from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi_cache.decorator import cache
from sqlalchemy import or_
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, distinct, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.common import build_filters
from offsets_db_api.database import get_session
from offsets_db_api.geo import (
    get_bbox_for_project,
    get_bboxes_for_projects,
    get_projects_with_geometry,
)
from offsets_db_api.log import get_logger
from offsets_db_api.models import (
    Clip,
    ClipProject,
    Credit,
    PaginatedProjects,
    Project,
    ProjectWithClips,
)
from offsets_db_api.schemas import (
    BeneficiaryFilters,
    Pagination,
    ProjectFilters,
    ProjectTypes,
    get_beneficiary_filters,
    get_project_filters,
)
from offsets_db_api.security import check_api_key
from offsets_db_api.sql_helpers import (
    apply_beneficiary_search,
    apply_filters,
    apply_sorting,
    expand_project_types,
    get_project_types,
    handle_pagination,
)

router = APIRouter()
logger = get_logger()


@router.get('/types', summary='Get project types', response_model=ProjectTypes)
@cache(namespace=CACHE_NAMESPACE)
async def get_grouped_project_types(
    request: Request,
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get project types"""
    logger.info(f'Getting project types: {request.url}')
    return get_project_types(session)


@router.get(
    '/', summary='Get projects with pagination and filtering', response_model=PaginatedProjects
)
@cache(namespace=CACHE_NAMESPACE)
async def get_projects(
    request: Request,
    project_filters: ProjectFilters = Depends(get_project_filters),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    beneficiary_filters: BeneficiaryFilters = Depends(get_beneficiary_filters),
    geography: bool = Query(
        False,
        description='Filter to only include projects that have geographic boundaries.',
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

    project_filters.project_type = expand_project_types(session, project_filters.project_type)
    # Base query without Credit join
    matching_projects = select(distinct(Project.project_id))

    filters = build_filters(project_filters=project_filters)

    # Filter to only projects with geographic boundaries
    if geography:
        projects_with_geo = get_projects_with_geometry()
        matching_projects = matching_projects.where(col(Project.project_id).in_(projects_with_geo))

    if search:
        search_pattern = f'%{search}%'
        matching_projects = matching_projects.where(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    if beneficiary_filters.beneficiary_search:
        Credit_alias = aliased(Credit)
        matching_projects = matching_projects.outerjoin(
            Credit_alias, col(Project.project_id) == col(Credit_alias.project_id)
        )
        matching_projects = apply_beneficiary_search(
            statement=matching_projects,
            search_term=beneficiary_filters.beneficiary_search,
            search_fields=beneficiary_filters.beneficiary_search_fields,
            credit_model=Credit_alias,
            project_model=Project,
        )
    matching_projects_select = select(matching_projects.subquery())

    # Use the subquery to filter the main query
    statement = select(Project).where(col(Project.project_id).in_(matching_projects_select))

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
    project_ids = [project.project_id for project in results]

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

    # Get bboxes for all project IDs
    project_bboxes = get_bboxes_for_projects(project_ids)

    # Transform the dictionary into a list of projects with clips, project_type, and bbox
    projects_with_clips = []
    for project in results:
        project_data = project.model_dump()
        project_data['clips'] = [
            clip.model_dump() for clip in project_to_clips.get(project.project_id, [])
        ]
        project_data['bbox'] = project_bboxes.get(project.project_id)
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
    statement = select(Project).where(Project.project_id == project_id)

    project = session.exec(statement).one_or_none()

    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f'project {project_id} not found'
        )

    # Subquery to get the related clips
    clip_statement = (
        select(Clip)
        .join(ClipProject, col(Clip.id) == col(ClipProject.clip_id))
        .where(ClipProject.project_id == project_id)
    )
    clip_projects_subquery = session.exec(clip_statement).all()

    # Construct the response data
    project_data = project.model_dump()

    project_data['clips'] = [clip.model_dump() for clip in clip_projects_subquery]
    project_data['bbox'] = get_bbox_for_project(project_id)
    return project_data
