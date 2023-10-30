import datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, or_

from ..database import get_session
from ..logging import get_logger
from ..models import Clip, ClipWithPagination
from ..query_helpers import apply_filters, apply_sorting, handle_pagination
from ..schemas import Pagination

router = APIRouter()
logger = get_logger()


@router.get('/', response_model=ClipWithPagination)
def get_clips(
    request: Request,
    project_id: list[str] | None = Query(None, description='Project ID'),
    article_type: list[str] | None = Query(None, description='Article type'),
    published_at_from: datetime.date
    | datetime.datetime
    | None = Query(None, description='Published at from'),
    published_at_to: datetime.date
    | datetime.datetime
    | None = Query(None, description='Published at to'),
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
    """
    Get clips associated with a project
    """
    logger.info(f'Getting clips: {request.url}')

    filters = [
        ('article_type', article_type, 'ilike', Clip),
        ('published_at', published_at_from, '>=', Clip),
        ('published_at', published_at_to, '<=', Clip),
        ('project_id', project_id, 'in', Clip),
    ]

    query = session.query(Clip)

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(Clip.project_id.ilike(search_pattern), Clip.title.ilike(search_pattern))
        )

    if sort:
        query = apply_sorting(query=query, sort=sort, model=Clip, primary_key='id')

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        query=query, current_page=current_page, per_page=per_page, request=request
    )

    return ClipWithPagination(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=results,
    )
