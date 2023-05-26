import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import or_
from sqlmodel import Session

from ..database import get_session
from ..logging import get_logger
from ..models import Credit, CreditRead
from ..query_helpers import apply_sorting

router = APIRouter()
logger = get_logger()


@router.get('/', summary='List credits', response_model=list[CreditRead])
def get_credits(
    project_id: list[str] | None = Query(None, description='Project ID'),
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    vintage: list[int] | None = Query(None, description='Vintage'),
    transaction_date_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    transaction_date_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    limit: int = Query(500, description='Limit number of results', le=1000, gt=0),
    offset: int = Query(0, description='Offset results', ge=0),
    sort: list[str] = Query(
        default=['project_id'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    session: Session = Depends(get_session),
):
    """List credits"""
    logger.info('Getting credits')

    query = session.query(Credit)

    if project_id:
        query = query.filter(or_(*[Credit.project_id.ilike(p) for p in project_id]))

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

    results = query.offset(offset).limit(limit).all()
    logger.info(f'Found {len(results)} credits')
    return results
