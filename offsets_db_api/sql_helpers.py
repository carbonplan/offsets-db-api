import datetime
import typing

from fastapi import HTTPException, Request
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import Session, SQLModel, and_, asc, desc, distinct, func, nullslast, or_, select
from sqlmodel.sql.expression import Select as _Select, SelectOfScalar

from offsets_db_api.models import Clip, ClipProject, Credit, File, Project
from offsets_db_api.query_helpers import _generate_next_page_url
from offsets_db_api.schemas import Registries


def apply_sorting(
    *,
    statement: _Select[typing.Any] | SelectOfScalar[typing.Any],
    sort: list[str],
    model: type[Credit | Project | Clip | ClipProject | File | SQLModel],
    primary_key: str,
) -> _Select[typing.Any] | SelectOfScalar[typing.Any]:
    # Define valid column names
    columns = [c.name for c in model.__table__.columns]

    # Ensure that the primary key field is always included in the sort parameters list to ensure consistent pagination
    if primary_key not in sort and f'-{primary_key}' not in sort and f'+{primary_key}' not in sort:
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
        statement = statement.order_by(nullslast(order(getattr(model, field))))

    return statement


def apply_filters(
    *,
    statement: _Select[typing.Any] | SelectOfScalar[typing.Any],
    model: type[Credit | Project | Clip | ClipProject | File | SQLModel],
    attribute: str,
    values: list[str] | None | int | datetime.date | list[Registries] | typing.Any,
    operation: str,
) -> _Select[typing.Any] | SelectOfScalar[typing.Any]:
    """
    Apply filters to the statement based on operation type.
    Supports 'ilike', '==', '>=', and '<=' operations.

    Parameters
    ----------
    statement: Select
        SQLAlchemy Select statement
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
    statement: Select
        updated SQLAlchemy Select statement
    """

    if values is not None:
        attr_type = getattr(model, attribute).type
        is_array = isinstance(attr_type, ARRAY)
        is_list = isinstance(values, list | tuple | set)

        if is_array and is_list:
            if operation == 'ALL':
                statement = statement.where(
                    and_(*[getattr(model, attribute).op('@>')(f'{{{v}}}') for v in values])
                )
            else:
                statement = statement.where(
                    or_(*[getattr(model, attribute).op('@>')(f'{{{v}}}') for v in values])
                )

        if operation == 'ilike':
            statement = (
                statement.where(or_(*[getattr(model, attribute).ilike(v) for v in values]))
                if is_list
                else statement.where(getattr(model, attribute).ilike(values))
            )
        elif operation == '==':
            statement = (
                statement.where(or_(*[getattr(model, attribute) == v for v in values]))
                if is_list
                else statement.where(getattr(model, attribute) == values)
            )
        elif operation == '>=':
            statement = (
                statement.where(or_(*[getattr(model, attribute) >= v for v in values]))
                if is_list
                else statement.where(getattr(model, attribute) >= values)
            )
        elif operation == '<=':
            statement = (
                statement.where(or_(*[getattr(model, attribute) <= v for v in values]))
                if is_list
                else statement.where(getattr(model, attribute) <= values)
            )

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
