from fastapi import APIRouter, Depends, Query, Request
from fastapi_cache.decorator import cache
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, func, or_, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.common import build_filters
from offsets_db_api.database import get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import Clip, ClipProject, PaginatedClips, Pagination, Project
from offsets_db_api.schemas import ClipFilters, get_clip_filters
from offsets_db_api.security import check_api_key
from offsets_db_api.sql_helpers import apply_filters, apply_sorting, handle_pagination

router = APIRouter()
logger = get_logger()


@router.get('/', response_model=PaginatedClips)
@cache(namespace=CACHE_NAMESPACE)
async def get_clips(
    request: Request,
    clip_filters: ClipFilters = Depends(get_clip_filters),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `title` fields only.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    sort: list[str] = Query(
        default=['date'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """
    Get clips associated with a project
    """
    logger.info(f'Getting clips: {request.url}')

    filters = build_filters(
        clip_filters=clip_filters,
    )

    # extract project_id filter if present
    project_id_filter = None
    for attribute, values, operation, model in filters:
        if attribute == 'project_id' and model == ClipProject:
            project_id_filter = values
            break

    # create subquery for project data
    project_data_subquery = select(
        col(ClipProject.clip_id).label('clip_id'),
        func.array_agg(
            func.json_build_object(
                'project_id',
                Project.project_id,
                'category',
                Project.category,
                'project_type',
                Project.project_type,
            )
        ).label('projects'),
    ).join(Project, col(ClipProject.project_id) == col(Project.project_id))

    # Apply project_id filter to the subquery if present
    if project_id_filter:
        project_data_subquery = project_data_subquery.where(
            col(ClipProject.project_id) == project_id_filter
        )

    # Complete the subquery
    project_data_subquery = project_data_subquery.group_by(col(ClipProject.clip_id)).subquery()
    # create an aliased name for the subquery
    project_data_subquery_alias = aliased(project_data_subquery, name='project_data')

    # construct the main query
    statement = select(
        Clip.date,
        Clip.title,
        Clip.url,
        Clip.source,
        Clip.tags,
        Clip.notes,
        Clip.is_waybacked,
        Clip.type,
        Clip.id,
        project_data_subquery_alias.c.projects,
    ).join(project_data_subquery_alias, col(Clip.id) == col(project_data_subquery_alias.c.clip_id))

    for attribute, values, operation, model in filters:
        statement = apply_filters(
            statement=statement,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        clip_project_alias = aliased(ClipProject)
        statement = statement.join(
            clip_project_alias,
            Clip.id == clip_project_alias.clip_id,
        ).filter(
            or_(
                col(clip_project_alias.project_id).ilike(search_pattern),
                col(Clip.title).ilike(search_pattern),
            )
        )

    if sort:
        statement = apply_sorting(statement=statement, sort=sort, model=Clip, primary_key='id')

    total_entries, current_page, total_pages, next_page, query_results = handle_pagination(
        statement=statement,
        primary_key=Clip.id,
        current_page=current_page,
        per_page=per_page,
        request=request,
        session=session,
    )

    clips_info = []
    for (
        date,
        title,
        url,
        source,
        tags,
        notes,
        is_waybacked,
        clip_type,
        clip_id,
        projects,
    ) in query_results:
        clip_info = {
            'date': date,
            'title': title,
            'url': url,
            'source': source,
            'tags': tags,
            'notes': notes,
            'is_waybacked': is_waybacked,
            'type': clip_type,
            'id': clip_id,
            'projects': projects,
        }

        clips_info.append(clip_info)

    pagination = Pagination(
        total_entries=total_entries,
        current_page=current_page,
        total_pages=total_pages,
        next_page=next_page,
    )

    return PaginatedClips(
        pagination=pagination,
        data=clips_info,
    )
