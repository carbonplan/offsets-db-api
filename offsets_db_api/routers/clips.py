import datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, or_

from ..database import get_session
from ..logging import get_logger
from ..models import Clip, ClipProject, PaginatedClips
from ..query_helpers import apply_filters, apply_sorting, handle_pagination
from ..schemas import Pagination

router = APIRouter()
logger = get_logger()


@router.get('/')
def get_clips(
    request: Request,
    project_id: list[str] | None = Query(None, description='Project ID'),
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
        default=['id'],
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
        ('tags', tags, 'ANY', Clip),
        ('date', date_from, '>=', Clip),
        ('date', date_to, '<=', Clip),
    ]

    query = session.query(Clip, ClipProject.project_id).join(
        ClipProject, Clip.id == ClipProject.clip_id
    )
    return query.all()
    # Handle 'project_id' filter separately due to its relationship
    if project_id:
        query = query.join(ClipProject).filter(ClipProject.project_id.in_(project_id))

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

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        query=query, current_page=current_page, per_page=per_page, request=request
    )
    return results

    return PaginatedClips(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=results,
    )
