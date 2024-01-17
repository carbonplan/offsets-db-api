import datetime

from fastapi import APIRouter, Depends, Query, Request
from fastapi_cache.decorator import cache
from sqlmodel import Session, col, or_

from ..cache import CACHE_NAMESPACE
from ..database import get_session
from ..logging import get_logger
from ..models import Clip, ClipProject, PaginatedClips, Project
from ..query_helpers import apply_filters, apply_sorting, handle_pagination
from ..schemas import Pagination
from ..security import check_api_key

router = APIRouter()
logger = get_logger()


@router.get('/', response_model=PaginatedClips)
@cache(namespace=CACHE_NAMESPACE)
async def get_clips(
    request: Request,
    project_id: list[str] | None = Query(None, description='Project ID'),
    source: list[str] | None = Query(None, description='Source'),
    tags: list[str] | None = Query(None, description='Tags'),
    type: list[str] | None = Query(None, description='Article type'),
    date_from: datetime.date | datetime.datetime | None = Query(
        None, description='Published at from'
    ),
    date_to: datetime.date | datetime.datetime | None = Query(None, description='Published at to'),
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

    filters = [
        ('type', type, 'ilike', Clip),
        ('source', source, 'ilike', Clip),
        ('tags', tags, 'ANY', Clip),
        ('date', date_from, '>=', Clip),
        ('date', date_to, '<=', Clip),
        ('project_id', project_id, '==', ClipProject),
    ]

    query = (
        session.query(Clip)
        .join(ClipProject, col(Clip.id) == col(ClipProject.clip_id))
        .join(Project, col(ClipProject.project_id) == col(Project.project_id), isouter=True)
    )

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(
                col(ClipProject.project_id).ilike(search_pattern),
                col(Clip.title).ilike(search_pattern),
            )
        )

    if sort:
        query = apply_sorting(query=query, sort=sort, model=Clip, primary_key='id')

    # TODO: Figure out how to handle pagination when joining multiple tables with many-to-many relations
    # temporary hard-code per_page to 300

    logger.info(f'Overriding per_page to {per_page}')
    per_page = 300

    _, current_page, total_pages, next_page, query_results = handle_pagination(
        query=query,
        primary_key=Clip.id,
        current_page=current_page,
        per_page=per_page,
        request=request,
    )

    # Collect clip information with associated projects and their categories
    clips_info = []
    for result in query_results:
        clip = result  # Assuming Clip is the first object returned by the query
        projects_info = []
        # Loop through the ClipProjects related to the clip to collect project info
        for clip_project in clip.project_relationships:
            project_info = {
                'project_id': clip_project.project_id,
                'category': clip_project.project.category if clip_project.project else [],
            }
            projects_info.append(project_info)

        clip_dict = clip.model_dump()
        clip_dict['projects'] = projects_info
        clips_info.append(clip_dict)

    pagination = Pagination(
        total_entries=len(clips_info),
        current_page=current_page,
        total_pages=total_pages,
        next_page=next_page,
    )

    return PaginatedClips(
        pagination=pagination,
        data=clips_info,
    )
