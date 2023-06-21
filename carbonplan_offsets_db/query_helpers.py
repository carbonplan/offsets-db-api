from urllib.parse import urlencode

from fastapi import HTTPException, Request
from sqlalchemy import asc, desc
from sqlalchemy.orm import Query

from .models import Credit, Project


def apply_sorting(*, query, sort: list[str], model):
    # Define valid column names
    columns = [c.name for c in model.__table__.columns]

    for sort_param in sort:
        sort_param = sort_param.strip()
        # Check if sort_param starts with '-' for descending order
        if sort_param.startswith('-'):
            order = desc
            field = sort_param[1:]  # Remove the '-' from sort_param

        elif sort_param.startswith('+'):
            order = asc
            field = sort_param[1:]  # Remove the '+' from sort_param
        else:
            order = asc
            field = sort_param

        # Check if field is a valid column name
        if field not in columns:
            raise HTTPException(
                status_code=400,
                detail=f'Invalid sort field: {field}. Must be one of {columns}',
            )

        # Apply sorting to the query
        query = query.order_by(order(getattr(model, field)))

    return query


def handle_pagination(
    *, query: Query, current_page: int, per_page: int, request: Request
) -> tuple[int, int, str | None, list[Project | Credit]]:
    """
    Calculate total records, pages and next page url for a given query

    Parameters
    ----------
    query: Query
        SQLAlchemy Query
    current_page: int
        Current page number
    per_page: int
        Number of records per page
    request: Request
        FastAPI request instance

    Returns
    -------
    total_entries: int
        Total records in query
    total_pages: int
        Total pages in query
    next_page: Optional[str]
        URL of next page
    results: List[Project | Credit]
        Results for the current page
    """

    # Calculate total and pages
    total_entries = query.count()
    total_pages = (total_entries + per_page - 1) // per_page  # ceil(total / per_page)

    # Calculate the next page URL
    next_page = None

    if current_page < total_pages:
        # Convert the QueryParams to a dict, update 'page' and convert to a URL encoded string
        query_params = dict(request.query_params)
        query_params['current_page'] = current_page + 1
        query_params['per_page'] = per_page
        query_string = urlencode(query_params)

        # Construct the next page URL
        next_page = f'{request.url.scheme}://{request.url.netloc}{request.url.path}?{query_string}'

    # Get the results for the current page
    data = query.offset((current_page - 1) * per_page).limit(per_page).all()

    return total_entries, current_page, total_pages, next_page, data
