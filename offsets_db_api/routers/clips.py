import datetime
from collections import defaultdict

from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, or_

from ..database import get_session
from ..logging import get_logger
from ..models import Clip, ClipProject, PaginatedClips, Project
from ..query_helpers import apply_filters, apply_sorting, handle_pagination
from ..schemas import Pagination

router = APIRouter()
logger = get_logger()


@router.get('/', response_model=PaginatedClips)
def get_clips(
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
        .join(ClipProject, Clip.id == ClipProject.clip_id)
        .join(Project, ClipProject.project_id == Project.project_id, isouter=True)
    )

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(ClipProject.project_id.ilike(search_pattern), Clip.title.ilike(search_pattern))
        )

    if sort:
        query = apply_sorting(query=query, sort=sort, model=Clip, primary_key='id')

    total_entries, current_page, total_pages, next_page, query_results = handle_pagination(
        query=query, current_page=current_page, per_page=per_page, request=request
    )

    # Group clips by their id and collect project IDs and categories
    clip_data_dict = defaultdict(lambda: {'clip': None, 'project_ids': set(), 'category': set()})
    for clip in query_results:
        clip_id = clip.id
        clip_data = clip_data_dict[clip_id]
        if clip_data['clip'] is None:
            clip_data['clip'] = clip
        # Here we gather project_ids and categories from the ClipProject relationship
        for clip_project in clip.project_relationships:
            clip_data['project_ids'].add(clip_project.project_id)
            if clip_project.project and clip_project.project.category:
                clip_data['category'].update(clip_project.project.category)

    # Convert to the desired list of dicts format including categories
    clips_with_projects_and_categories = [
        {
            **clip_data['clip'].dict(),
            'project_ids': list(clip_data['project_ids']),
            'category': list(clip_data['category']),
        }
        for clip_data in clip_data_dict.values()
    ]

    return PaginatedClips(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=clips_with_projects_and_categories,
    )
