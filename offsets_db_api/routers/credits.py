import datetime

from fastapi import APIRouter, Depends, Query, Request
from fastapi_cache.decorator import cache
from sqlmodel import Session, col, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.database import get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import Credit, PaginatedCredits, Project
from offsets_db_api.schemas import Pagination, Registries
from offsets_db_api.security import check_api_key
from offsets_db_api.sql_helpers import (
    apply_beneficiary_search,
    apply_filters,
    apply_sorting,
    handle_pagination,
)

router = APIRouter()
logger = get_logger()


@router.get('/', summary='List credits', response_model=PaginatedCredits)
@cache(namespace=CACHE_NAMESPACE)
async def get_credits(
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
    beneficiary_search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches in fields specified in `search_fileds` parameter',
    ),
    beneficiary_search_fields: list[str] = Query(
        default=[
            'retirement_beneficiary',
            'retirement_account',
            'retirement_note',
            'retirement_reason',
        ],
        description='Fields to search in',
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
    statement = select(Credit, Project.category).join(
        Project, col(Credit.project_id) == col(Project.project_id), isouter=True
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
        filters.insert(0, ('project_id', project_id, '==', Project))

    for attribute, values, operation, model in filters:
        statement = apply_filters(
            statement=statement,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    if beneficiary_search:
        statement = apply_beneficiary_search(
            statement=statement,
            search_term=beneficiary_search,
            search_fields=beneficiary_search_fields,
            credit_model=Credit,
            project_model=Project,
        )

    if sort:
        statement = apply_sorting(statement=statement, sort=sort, model=Credit, primary_key='id')

    logger.info(f'SQL Credits Query: {statement.compile(compile_kwargs={"literal_binds": True})}')

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        statement=statement,
        primary_key=Credit.id,
        current_page=current_page,
        per_page=per_page,
        request=request,
        session=session,
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
