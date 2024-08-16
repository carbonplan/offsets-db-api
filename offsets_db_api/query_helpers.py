import typing
from urllib.parse import quote

import sqlmodel
from fastapi import HTTPException, Request
from sqlalchemy.orm import Query
from sqlmodel import and_, asc, desc, distinct, func, nullslast, or_, select

from .log import get_logger
from .models import Clip, ClipProject, Credit, Project

logger = get_logger()


def apply_filters(
    *,
    query,
    model: Credit | Project | Clip | ClipProject,
    attribute: str,
    values: list,
    operation: str,
):
    """
    Apply filters to the query based on operation type.
    Supports 'ilike', '==', '>=', and '<=' operations.

    Parameters
    ----------
    query: Query
        SQLAlchemy Query
    model: Credit | Project | Clip | ClipProject
        SQLAlchemy model class
    attribute: str
        model attribute to apply filter on
    values: list
        list of values to filter with
    operation: str
        operation type to apply to the filter ('ilike', '==', '>=', '<=')


    Returns
    -------
    query: Query
        updated SQLAlchemy Query object
    """

    if values is not None:
        attr_type = getattr(model, attribute).prop.columns[0].type
        is_array = str(attr_type).startswith('ARRAY')
        # Check if values is a list
        is_list = isinstance(values, list | tuple | set)

        if is_array and is_list:
            if operation == 'ALL':
                query = query.filter(
                    and_(*[getattr(model, attribute).op('@>')(f'{{{v}}}') for v in values])
                )
            else:
                query = query.filter(
                    or_(*[getattr(model, attribute).op('@>')(f'{{{v}}}') for v in values])
                )

        if operation == 'ilike':
            query = (
                query.filter(or_(*[getattr(model, attribute).ilike(v) for v in values]))
                if is_list
                else query.filter(getattr(model, attribute).ilike(values))
            )
        elif operation == '==':
            query = (
                query.filter(or_(*[getattr(model, attribute) == v for v in values]))
                if is_list
                else query.filter(getattr(model, attribute) == values)
            )
        elif operation == '>=':
            query = (
                query.filter(or_(*[getattr(model, attribute) >= v for v in values]))
                if is_list
                else query.filter(getattr(model, attribute) >= values)
            )
        elif operation == '<=':
            query = (
                query.filter(or_(*[getattr(model, attribute) <= v for v in values]))
                if is_list
                else query.filter(getattr(model, attribute) <= values)
            )

    return query


def apply_sorting(*, query, sort: list[str], model, primary_key: str):
    # Define valid column names
    columns = [c.name for c in model.__table__.columns]
    # Ensure that the primary key field is always included in the sort parameters list to ensure consistent pagination
    if primary_key not in sort or f'-{primary_key}' not in sort or f'+{primary_key}' not in sort:
        sort.append(primary_key)

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
        query = query.order_by(nullslast(order(getattr(model, field))))

    return query


def handle_pagination(
    *,
    query: Query,
    primary_key,
    current_page: int,
    per_page: int,
    request: Request,
    session: sqlmodel.Session | None = None,
) -> tuple[
    int, int, int, str | None, list[Credit | Project | Clip | ClipProject | dict[str, typing.Any]]
]:
    """
    Calculate total records, pages and next page url for a given query

    Parameters
    ----------
    query: Query
        SQLAlchemy Query

    primary_key
        Primary key field for distinct count
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
    results: List[SQLModel]
        Results for the current page
    """

    if isinstance(query, sqlmodel.sql.expression.Select):
        pk_column = primary_key if isinstance(primary_key, str) else primary_key.key
        count_query = select(
            func.count(distinct(getattr(query.selected_columns, pk_column)))
        ).select_from(query.subquery())
        total_entries = session.exec(count_query).one()

    else:
        # Create a separate count query without ORDER BY
        count_query = query.with_entities(func.count(distinct(primary_key))).order_by(None)
        total_entries = count_query.scalar()
    total_pages = (total_entries + per_page - 1) // per_page  # ceil(total / per_page)

    # Calculate the next page URL
    next_page = None

    if current_page < total_pages:
        next_page = _generate_next_page_url(
            request=request, current_page=current_page, per_page=per_page
        )
    # Get the results for the current page
    paginated_query = query.offset((current_page - 1) * per_page).limit(per_page)
    if isinstance(query, sqlmodel.sql.expression.Select):
        data = session.exec(paginated_query).all()
    else:
        data = paginated_query.all()

    return total_entries, current_page, total_pages, next_page, data


def custom_urlencode(params):
    """
    Custom URL encoding function that handles list-type query parameters.

    Parameters
    ----------
    params : dict
        The query parameters to encode.

    Returns
    -------
    str
        The URL-encoded query string.
    """
    encoded = []
    for key, value in params.items():
        key = quote(str(key))
        if isinstance(value, list):
            # Extend list with multiple key-value pairs for list items
            encoded.extend(f"{key}={quote(str(v), safe='')}" for v in value)
        else:
            # Append single key-value pair
            encoded.append(f"{key}={quote(str(value), safe='')}")
    return '&'.join(encoded)


def _convert_query_params_to_dict(request):
    # Convert the QueryParams to a dict, preserving list-type values
    query_params = {}
    for key, value in request.query_params.multi_items():
        if key in query_params:
            if isinstance(query_params[key], list):
                query_params[key].append(value)
            else:
                query_params[key] = [query_params[key], value]
        else:
            query_params[key] = value

    return query_params


def _generate_next_page_url(*, request, current_page, per_page):
    """
    Generate the URL for the next page in pagination.

    Parameters
    ----------
    request : Request
        The current FastAPI request instance.
    current_page : int
        The current page number.
    per_page : int
        Number of records per page.

    Returns
    -------
    str
        The URL for the next page.
    """
    # Convert the QueryParams to a dict, preserving list-type values
    query_params = _convert_query_params_to_dict(request)

    # Update 'current_page' and 'per_page' for the next page
    query_params['current_page'] = current_page + 1
    query_params['per_page'] = per_page

    # Generate the URL-encoded query string
    query_string = custom_urlencode(query_params)

    return f'{request.url.scheme}://{request.url.netloc}{request.url.path}?{query_string}'
