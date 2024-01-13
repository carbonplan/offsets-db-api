import datetime

from fastapi import APIRouter, Depends, Query, Request
from fastapi_cache.decorator import cache
from sqlmodel import Session, or_

from ..cache import CACHE_NAMESPACE
from ..database import get_session
from ..logging import get_logger
from ..models import Credit, PaginatedCredits, Project
from ..query_helpers import apply_filters, apply_sorting, handle_pagination
from ..schemas import Pagination, Registries
from ..security import check_api_key

router = APIRouter()
logger = get_logger()


@router.get('/', summary='List credits', response_model=PaginatedCredits)
@cache(namespace=CACHE_NAMESPACE)
def get_credits(
    request: Request,
    project_id: list[str] | None = Query(None, description='Project ID'),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    vintage: list[int] | None = Query(None, description='Vintage'),
    transaction_date_from: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    transaction_date_to: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    sort: list[str] = Query(
        default=['project_id'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """List credits"""
    logger.info(f'Getting credits: {request.url}')

    # Outer join to get all credits, even if they don't have a project
    query = session.query(Credit, Project.category).join(
        Project, Credit.project_id == Project.project_id, isouter=True
    )

    filters = [
        ('registry', registry, 'ilike', Project),
        ('transaction_type', transaction_type, 'ilike', Credit),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('vintage', vintage, '==', Credit),
        ('transaction_date', transaction_date_from, '>=', Credit),
        ('transaction_date', transaction_date_to, '<=', Credit),
    ]

    # Filter for project_id
    if project_id:
        # insert at the beginning of the list to ensure that it is applied first
        filters.insert(0, ('project_id', project_id, '==', Project))

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    if sort:
        query = apply_sorting(query=query, sort=sort, model=Credit, primary_key='id')

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        query=query,
        primary_key=Credit.id,
        current_page=current_page,
        per_page=per_page,
        request=request,
    )

    credits_with_category = [
        {
            **credit.model_dump(),
            'projects': [{'project_id': credit.project_id, 'category': category}],
        }
        for credit, category in results
    ]

    return PaginatedCredits(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=credits_with_category,
    )
