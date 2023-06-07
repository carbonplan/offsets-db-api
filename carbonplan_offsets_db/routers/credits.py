import datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import or_
from sqlmodel import Session

from ..database import get_session
from ..logging import get_logger
from ..models import Credit, CreditWithPagination, Project
from ..query_helpers import apply_sorting, handle_pagination
from ..schemas import Pagination, Registries

router = APIRouter()
logger = get_logger()


@router.get('/', summary='List credits', response_model=CreditWithPagination)
def get_credits(
    request: Request,
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
    page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
):
    """List credits"""
    logger.info('Getting credits')

    # join Credit with Project on project_id
    query = session.query(Credit).join(Project, Credit.project_id == Project.project_id)

    if registry:
        query = query.filter(or_(*[Project.registry.ilike(r) for r in registry]))

    if category:
        query = query.filter(or_(*[Project.category.ilike(c) for c in category]))

    if is_arb is not None:
        query = query.filter(Project.is_arb == is_arb)

    if search:
        search_pattern = (
            f'%{search}%'  # Wrapping search string with % to match anywhere in the string
        )
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    if transaction_type:
        query = query.filter(or_(*[Credit.transaction_type.ilike(t) for t in transaction_type]))

    if vintage:
        query = query.filter(or_(*[Credit.vintage == v for v in vintage]))

    if transaction_date_from:
        query = query.filter(Credit.transaction_date >= transaction_date_from)

    if transaction_date_to:
        query = query.filter(Credit.transaction_date <= transaction_date_to)

    if sort:
        query = apply_sorting(query=query, sort=sort, model=Credit)

    total, page, pages, next_page, results = handle_pagination(
        query=query, page=page, per_page=per_page, request=request
    )

    return CreditWithPagination(
        pagination=Pagination(total=total, page=page, pages=pages, next_page=next_page),
        data=results,
    )
