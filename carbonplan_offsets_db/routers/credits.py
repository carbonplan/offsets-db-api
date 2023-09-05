import datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import or_
from sqlmodel import Session

from ..database import get_session
from ..logging import get_logger
from ..models import Credit, CreditStats, CreditStatsWithPagination, CreditWithPagination, Project
from ..query_helpers import apply_filters, apply_sorting, handle_pagination
from ..schemas import Pagination, Registries

router = APIRouter()
logger = get_logger()


@router.get('/', summary='List credits', response_model=CreditWithPagination)
def get_credits(
    request: Request,
    project_id: list[str] | None = Query(None, description='Project ID'),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_arb: bool | None = Query(None, description='Whether project is an ARB project'),
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    vintage: list[int] | None = Query(None, description='Vintage'),
    transaction_date_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    transaction_date_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    search: str
    | None = Query(
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
):
    """List credits"""
    logger.info(f'Getting credits: {request.url}')

    # join Credit with Project on project_id
    query = session.query(Credit).join(Project, Credit.project_id == Project.project_id)

    # Filter for project_id
    if project_id:
        query = apply_filters(
            query=query, model=Credit, attribute='project_id', values=project_id, operation='=='
        )

    # Filters applying 'ilike' operation
    ilike_filters = [
        ('registry', registry, 'ilike', Project),
        ('category', category, 'ilike', Project),
        ('transaction_type', transaction_type, 'ilike', Credit),
    ]

    for attribute, values, operation, model in ilike_filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Filter applying '==' operation
    equal_filters = [('is_arb', is_arb, '==', Project), ('vintage', vintage, '==', Credit)]

    for attribute, values, operation, model in equal_filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Filters applying '>=' or '<=' operations
    date_filters = [
        ('transaction_date', transaction_date_from, '>=', Credit),
        ('transaction_date', transaction_date_to, '<=', Credit),
    ]

    for attribute, values, operation, model in date_filters:
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
        query = apply_sorting(query=query, sort=sort, model=Credit)

    logger.info(request.query_params.multi_items())

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        query=query, current_page=current_page, per_page=per_page, request=request
    )

    return CreditWithPagination(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=results,
    )


@router.get(
    '/stats/',
    response_model=CreditStatsWithPagination,
    summary='Get aggregated credits statistics',
)
def get_credit_stats(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    date_from: datetime.date | None = Query(default=None, description='Format: YYYY-MM-DD'),
    date_to: datetime.date | None = Query(default=None, description='Format: YYYY-MM-DD'),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    sort: list[str] = Query(
        default=['registry'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    session: Session = Depends(get_session),
):
    """
    Returns a list of CreditStats objects containing aggregated statistics for all credits in the database.
    """
    logger.info('Getting credits stats')

    query = session.query(CreditStats)

    # Filters applying 'ilike' operation
    ilike_filters = [
        ('registry', registry, 'ilike', CreditStats),
        ('transaction_type', transaction_type, 'ilike', CreditStats),
    ]

    for attribute, values, operation, model in ilike_filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Filters applying '>=' or '<=' operations
    date_filters = [('date', date_from, '>=', CreditStats), ('date', date_to, '<=', CreditStats)]

    for attribute, values, operation, model in date_filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    if sort:
        query = apply_sorting(query=query, sort=sort, model=CreditStats)

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        query=query, current_page=current_page, per_page=per_page, request=request
    )

    return CreditStatsWithPagination(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=results,
    )
