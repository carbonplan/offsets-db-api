import typing
from urllib.parse import quote

from fastapi import HTTPException, Request
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import (
    Session,
    SQLModel,
    String,
    Text,
    and_,
    asc,
    desc,
    distinct,
    func,
    nullslast,
    or_,
    select,
)
from sqlmodel.sql.expression import Select as _Select, SelectOfScalar

from offsets_db_api.models import Clip, ClipProject, Credit, Project
from offsets_db_api.schemas import ProjectTypes


def apply_sorting(
    *,
    statement: _Select[typing.Any] | SelectOfScalar[typing.Any],
    sort: list[str],
    model: type[SQLModel],
    primary_key: str,
) -> _Select[typing.Any] | SelectOfScalar[typing.Any]:
    # Define valid column names
    columns = [c.name for c in model.__table__.columns]

    # Ensure that the primary key field is always included in the sort parameters list to ensure consistent pagination
    if all(s.lstrip('+-') != primary_key for s in sort):
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

        # Apply sorting to the statement
        column = getattr(model, field, None)
        if column is None:
            raise HTTPException(
                status_code=400,
                detail=f'Invalid sort field: {field}. Column not found in model.',
            )

        if isinstance(column.type, String | Text):
            # Case-insensitive sort for string fields
            statement = statement.order_by(nullslast(order(func.lower(column))))
        else:
            statement = statement.order_by(nullslast(order(column)))
    return statement


def apply_filters(
    *,
    statement: typing.Any,  # _Select or SelectOfScalar
    model: type[SQLModel],
    attribute: str,
    values: typing.Any,  # can be a single value or an iterable
    operation: str,
) -> typing.Any:
    """
    Apply filters to the statement based on operation type.
    Supports 'ilike', '==', '>=', '<=' and a special 'ALL' for ARRAY types.

    Parameters
    ----------
    statement : SQLAlchemy Select statement
    model : SQLModel subclass
        The model containing the column to filter.
    attribute : str
        The attribute (column name) to filter.
    values : any
        A single value or an iterable of values to filter by.
    operation : str
        The operation to use ('ilike', '==', '>=', '<=', or 'ALL' for ARRAY fields).

    Returns
    -------
    statement : Updated SQLAlchemy Select statement
    """
    if values is None:
        return statement

    column = getattr(model, attribute)
    is_iterable = isinstance(values, list | tuple | set)

    # Special handling for ARRAY types (PostgreSQL)
    if isinstance(column.type, ARRAY) and is_iterable:
        conditions = [column.op('@>')(f'{{{v}}}') for v in values]
        if operation == 'ALL':
            statement = statement.where(and_(*conditions))
        else:
            statement = statement.where(or_(*conditions))
        return statement

    # Mapping for standard operations
    operations_map = {
        'ilike': lambda col, v: col.ilike(v),
        '==': lambda col, v: col == v,
        '>=': lambda col, v: col >= v,
        '<=': lambda col, v: col <= v,
    }
    if operation not in operations_map:
        raise ValueError(
            f'Unsupported operation: {operation}. Supported operations: {list(operations_map.keys())}',
        )

    op_func = operations_map[operation]

    if is_iterable:
        conditions = [op_func(column, v) for v in values]
        statement = statement.where(or_(*conditions))
    else:
        statement = statement.where(op_func(column, values))
    return statement


def handle_pagination(
    *,
    statement: _Select[typing.Any] | SelectOfScalar[typing.Any],
    primary_key: typing.Any,
    current_page: int,
    per_page: int,
    request: Request,
    session: Session,
) -> tuple[
    int,
    int,
    int,
    str | None,
    typing.Iterable[Project | Clip | ClipProject | Credit],
]:
    """
    Calculate total records, pages and next page URL for a given query.

    Parameters
    ----------
    statement: Select
        SQLAlchemy Select statement
    primary_key
        Primary key field for distinct count
    current_page: int
        Current page number
    per_page: int
        Number of records per page
    request: Request
        FastAPI request instance
    session: Session
        SQLAlchemy session instance

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

    pk_column = primary_key if isinstance(primary_key, str) else primary_key.key
    count_query = select(func.count(distinct(getattr(statement.columns, pk_column))))
    total_entries = session.exec(count_query).one()

    total_pages = (total_entries + per_page - 1) // per_page  # ceil(total / per_page)

    # Calculate the next page URL
    next_page = None

    if current_page < total_pages:
        next_page = _generate_next_page_url(
            request=request, current_page=current_page, per_page=per_page
        )

    # Get the results for the current page
    paginated_statement = statement.offset((current_page - 1) * per_page).limit(per_page)
    results = session.exec(paginated_statement).all()

    return total_entries, current_page, total_pages, next_page, results


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
            encoded.extend(f'{key}={quote(str(v), safe="")}' for v in value)
        else:
            # Append single key-value pair
            encoded.append(f'{key}={quote(str(value), safe="")}')
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


def apply_beneficiary_search(
    *,
    statement: _Select[typing.Any],
    search_term: str,
    search_fields: list[str],
    credit_model: type[Credit],
    project_model: type[Project],
) -> _Select[typing.Any]:
    if not search_term:
        return statement
    search_pattern = f'%{search_term}%'
    search_conditions = []

    # Loop through fields to create search conditions
    for field in search_fields:
        if hasattr(credit_model, field):
            search_conditions.append(getattr(credit_model, field).ilike(search_pattern))
        elif hasattr(project_model, field):
            search_conditions.append(getattr(project_model, field).ilike(search_pattern))

    if search_conditions:
        statement = statement.where(or_(*search_conditions))
    return statement


def get_project_types(session: Session) -> ProjectTypes:
    top_n = 5
    statement = (
        select(Project.project_type, func.sum(Project.issued).label('total_issued'))
        .group_by(Project.project_type)
        .order_by(func.sum(Project.issued).desc())
    )

    result = session.exec(statement).all()
    top = [project_type for project_type, _ in result[:top_n]]
    others = [project_type for project_type, _ in result[top_n:]]
    return ProjectTypes(Top=top, Other=others)


def expand_project_types(session: Session, project_type: list[str] | None) -> list[str]:
    if not project_type:
        return project_type

    new_project_type = project_type.copy()
    if 'Other' in new_project_type:
        project_types = get_project_types(session)
        new_project_type.remove('Other')
        new_project_type.extend(project_types.Other)

    return new_project_type
