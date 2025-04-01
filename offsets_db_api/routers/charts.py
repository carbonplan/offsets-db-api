import datetime
import typing

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, Query, Request
from fastapi_cache.decorator import cache
from sqlalchemy.orm import aliased
from sqlmodel import Date, Session, and_, case, cast, col, distinct, func, literal, or_, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.common import build_filters
from offsets_db_api.database import get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import (
    Credit,
    PaginatedBinnedCreditTotals,
    PaginatedBinnedValues,
    PaginatedCreditCounts,
    PaginatedProjectCounts,
    PaginatedProjectCreditTotals,
    Project,
)
from offsets_db_api.schemas import (
    BeneficiaryFilters,
    CreditFilters,
    Pagination,
    ProjectFilters,
    get_beneficiary_filters,
    get_credit_filters,
    get_project_filters,
)
from offsets_db_api.security import check_api_key
from offsets_db_api.sql_helpers import apply_beneficiary_search, apply_filters, expand_project_types

router = APIRouter()
logger = get_logger()


def calculate_end_date(start_date, freq):
    """Calculate the end date based on the start date and frequency."""

    offset_mapping = {
        'D': pd.DateOffset(days=1),
        'W': pd.DateOffset(weeks=1),
        'M': pd.DateOffset(months=1),
        'Y': pd.DateOffset(years=1),
    }

    end_date = start_date + offset_mapping[freq]
    if freq in ['M', 'Y']:
        end_date -= pd.DateOffset(days=1)

    return end_date


def generate_date_bins(
    *,
    min_value,
    max_value,
    freq: typing.Literal['D', 'W', 'M', 'Y'] | None = None,
    num_bins: int | None = None,
):
    """
    Generate date bins with the specified frequency.

    Parameters
    ----------
    min_value : datetime.date
        The minimum date value.
    max_value : datetime.date
        The maximum date value.
    freq : str
        The frequency for binning. Can be 'D', 'W', 'M', or 'Y'.
    num_bins : int
        The number of bins to generate. If None, bins will be generated based on the frequency.

    Returns
    -------
    pd.DatetimeIndex
        The generated date bins.
    """

    # freq and num_bins are mutually exclusive
    if freq and num_bins:
        raise ValueError('freq and num_bins are mutually exclusive')

    min_value, max_value = (
        pd.Timestamp(min_value),
        pd.Timestamp(max_value),
    )

    # Adjust min_value based on frequency
    if freq == 'M':
        min_value = min_value.replace(day=1)
    elif freq == 'Y':
        min_value = min_value.replace(month=1, day=1)

    if num_bins:
        # Generate 'num_bins' bins
        date_bins = pd.date_range(start=min_value, end=max_value, periods=num_bins, normalize=True)
    else:
        frequency_mapping = {'Y': 'AS', 'M': 'MS', 'W': 'W', 'D': 'D'}
        date_bins = pd.date_range(
            start=min_value,
            end=max_value,
            freq=frequency_mapping[freq],
            normalize=True,
        )

    # Append the necessary last bin based on the frequency
    if freq == 'M':
        last_bin = (date_bins[-1] + pd.DateOffset(months=1)).replace(day=1)
    elif freq == 'Y':
        last_bin = (date_bins[-1] + pd.DateOffset(years=1)).replace(month=1, day=1)
    else:
        last_bin = max_value

    if date_bins[-1] != last_bin:
        date_bins = date_bins.append(pd.DatetimeIndex([last_bin]))

    logger.info(f'✅ Bins generated successfully: {date_bins}')
    return date_bins


def generate_dynamic_numeric_bins(*, min_value, max_value, bin_width=None):
    """Generate numeric bins with dynamically adjusted bin width."""
    # Check for edge cases where min and max are the same
    if min_value == max_value:
        return np.array([min_value])

    if bin_width is None:
        value_range = max_value - min_value
        order_of_magnitude = int(np.floor(np.log10(value_range)))
        bin_width = 10 ** (order_of_magnitude - 1)

    # Round min and max to nearest multiple of bin_width
    rounded_min = np.floor(min_value / bin_width) * bin_width
    rounded_max = np.ceil(max_value / bin_width) * bin_width
    # Generate evenly spaced values using the determined bin width
    numeric_bins = np.arange(rounded_min, rounded_max + bin_width, bin_width).astype(int)

    logger.info(f'🔢 Binning by numeric value with {len(numeric_bins)} bins, width: {bin_width}...')
    return numeric_bins


@router.get('/projects_by_listing_date', response_model=PaginatedBinnedValues)
@cache(namespace=CACHE_NAMESPACE)
async def get_projects_by_listing_date(
    request: Request,
    freq: typing.Literal['D', 'W', 'M', 'Y'] = Query('Y', description='Frequency of bins'),
    project_filters: ProjectFilters = Depends(get_project_filters),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get aggregated project registration data"""

    logger.info(f'Getting project registration data: {request.url}')

    project_filters.type = expand_project_types(session, project_filters.type)

    # Start with the base query on the Project model
    query = select(Project)

    # Apply filters
    filters = build_filters(project_filters=project_filters)

    for attribute, values, operation, model in filters:
        query = apply_filters(
            statement=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.where(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    # Now create the subquery
    subquery = query.alias('subquery')
    # Get min and max listing dates
    min_max_query = select(func.min(subquery.c.listed_at), func.max(subquery.c.listed_at))
    min_date, max_date = session.exec(min_max_query.select_from(subquery)).fetchone()

    if min_date is None or max_date is None:
        logger.info('✅ No data to bin!')
        return PaginatedBinnedValues(
            pagination=Pagination(
                total_entries=0,
                total_pages=1,
                next_page=None,
                current_page=current_page,
            ),
            data=[],
        )

    # Generate date bins using the original function
    date_bins = generate_date_bins(min_value=min_date, max_value=max_date, freq=freq).tolist()

    # Create a CASE statement for binning
    bin_case = case(
        *[
            (
                and_(subquery.c.listed_at >= bin_start, subquery.c.listed_at < bin_end),
                cast(bin_start, Date),
            )
            for bin_start, bin_end in zip(date_bins[:-1], date_bins[1:])
        ],
        else_=cast(date_bins[-1], Date),
    ).label('bin')

    # Add binning to the query and aggregate
    binned_query = (
        select(
            bin_case,
            subquery.c.category,
            func.count(subquery.c.project_id.distinct()).label('value'),
        )
        .select_from(subquery)
        .group_by(bin_case, subquery.c.category)
    )

    # Execute the query
    results = session.exec(binned_query).fetchall()

    # Format the results
    formatted_results = []
    current_year = datetime.datetime.now().year
    for row in results:
        start_date = row.bin
        if start_date.year > current_year:
            continue  # Skip future dates
        end_date = calculate_end_date(start_date, freq)
        formatted_results.append(
            {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d'),
                'category': row.category,
                'value': int(row.value),
            }
        )

    # Sort the results
    formatted_results.sort(key=lambda x: (x['start'], x['category']))

    return PaginatedBinnedValues(
        pagination=Pagination(
            total_entries=len(formatted_results),
            total_pages=1,
            next_page=None,
            current_page=current_page,
        ),
        data=formatted_results,
    )


@router.get('/credits_by_transaction_date', response_model=PaginatedBinnedValues)
@cache(namespace=CACHE_NAMESPACE)
async def get_credits_by_transaction_date(
    request: Request,
    freq: typing.Literal['D', 'W', 'M', 'Y'] = Query('Y', description='Frequency of bins'),
    project_filters: ProjectFilters = Depends(get_project_filters),
    credit_filters: CreditFilters = Depends(get_credit_filters),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get aggregated credit transaction data"""

    logger.info(f'Getting credit transaction data: {request.url}')

    project_filters.type = expand_project_types(session, project_filters.type)

    # Base query
    base_query = select(Credit, Project).join(
        Project, col(Credit.project_id) == col(Project.project_id)
    )

    filters = build_filters(
        project_filters=project_filters,
        credit_filters=credit_filters,
    )

    for attribute, values, operation, model in filters:
        base_query = apply_filters(
            statement=base_query,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        base_query = base_query.where(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    # Create the subquery with unnested category
    subquery = base_query.add_columns(
        Credit.transaction_date,
        Credit.quantity,
        Project.category,
    ).alias('subquery')

    # Get min and max transaction dates
    min_max_query = select(
        func.min(subquery.c.transaction_date), func.max(subquery.c.transaction_date)
    )
    min_date, max_date = session.exec(min_max_query.select_from(subquery)).fetchone()

    if min_date is None or max_date is None:
        logger.info('✅ No data to bin!')
        return PaginatedBinnedValues(
            pagination=Pagination(
                total_entries=0,
                total_pages=1,
                next_page=None,
                current_page=current_page,
            ),
            data=[],
        )

    # Generate date bins using the original function
    date_bins = generate_date_bins(min_value=min_date, max_value=max_date, freq=freq).tolist()

    # Create a CASE statement for binning
    bin_case = case(
        *[
            (
                and_(
                    subquery.c.transaction_date >= bin_start, subquery.c.transaction_date < bin_end
                ),
                cast(bin_start, Date),
            )
            for bin_start, bin_end in zip(date_bins[:-1], date_bins[1:])
        ],
        else_=cast(date_bins[-1], Date),
    ).label('bin')

    # Add binning to the query and aggregate
    binned_query = (
        select(bin_case, subquery.c.category, func.sum(subquery.c.quantity).label('value'))
        .select_from(subquery)
        .group_by(bin_case, subquery.c.category)
    )

    # Execute the query
    results = session.exec(binned_query).fetchall()

    # Format the results
    formatted_results = []
    current_year = datetime.datetime.now().year
    for row in results:
        start_date = row.bin
        if start_date.year > current_year:
            continue  # Skip future dates
        end_date = calculate_end_date(start_date, freq)
        formatted_results.append(
            {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d'),
                'category': row.category,
                'value': int(row.value),
            }
        )

    # Sort the results
    formatted_results.sort(key=lambda x: (x['start'], x['category']))

    return PaginatedBinnedValues(
        pagination=Pagination(
            total_entries=len(formatted_results),
            total_pages=1,
            next_page=None,
            current_page=current_page,
        ),
        data=formatted_results,
    )


@router.get(
    '/credits_by_transaction_date/{project_id}', response_model=PaginatedProjectCreditTotals
)
@cache(namespace=CACHE_NAMESPACE)
async def get_credits_by_project_id(
    request: Request,
    project_id: str,
    credit_filters: CreditFilters = Depends(get_credit_filters),
    beneficiary_filters: BeneficiaryFilters = Depends(get_beneficiary_filters),
    freq: typing.Literal['D', 'W', 'M', 'Y'] = Query('Y', description='Frequency of bins'),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get aggregated credit transaction data"""

    logger.info(f'Getting credit transaction data: {request.url}')

    # Base query
    query = (
        select(
            col(Credit.transaction_date),
            col(Credit.quantity),
            col(Credit.vintage),
            col(Credit.retirement_account),
            col(Credit.retirement_beneficiary),
            col(Credit.retirement_note),
            col(Credit.retirement_reason),
        )
        .join(Project)
        .where(Project.project_id == project_id)
    )

    filters = build_filters(credit_filters=credit_filters)

    for attribute, values, operation, model in filters:
        query = apply_filters(
            statement=query,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    if beneficiary_filters.beneficiary_search:
        query = apply_beneficiary_search(
            statement=query,
            search_term=beneficiary_filters.beneficiary_search,
            search_fields=beneficiary_filters.beneficiary_search_fields,
            credit_model=Credit,
            project_model=Project,
        )

    # Get min and max transaction dates from the filtered query
    min_max_subquery = query.subquery()
    min_max_query = select(
        func.min(min_max_subquery.c.transaction_date), func.max(min_max_subquery.c.transaction_date)
    )
    min_date, max_date = session.exec(min_max_query).fetchone()

    if min_date is None or max_date is None:
        logger.info('✅ No data to bin!')
        return PaginatedProjectCreditTotals(
            pagination=Pagination(
                total_entries=0,
                total_pages=1,
                next_page=None,
                current_page=current_page,
            ),
            data=[],
        )

    # Determine frequency if not provided
    if freq is None:
        date_diff = max_date - min_date
        if date_diff < datetime.timedelta(days=7):
            freq = 'D'
        elif date_diff < datetime.timedelta(days=30):
            freq = 'W'
        elif date_diff < datetime.timedelta(days=365):
            freq = 'M'
        else:
            freq = 'Y'

        if min_date.year == max_date.year:
            freq = 'M' if min_date.month == max_date.month else 'Y'
    # Generate date bins
    date_bins = generate_date_bins(min_value=min_date, max_value=max_date, freq=freq).tolist()

    # Create a CASE statement for binning
    bin_case = case(
        *[
            (
                and_(Credit.transaction_date >= bin_start, Credit.transaction_date < bin_end),
                cast(bin_start, Date),
            )
            for bin_start, bin_end in zip(date_bins[:-1], date_bins[1:])
        ],
        else_=cast(date_bins[-1], Date),
    ).label('bin')

    # Add binning to the query and aggregate
    binned_query = (
        query.add_columns(bin_case)
        .group_by('bin')
        .with_only_columns(bin_case, func.sum(Credit.quantity).label('value'))
        .order_by('bin')
    )

    # Execute the query
    results = session.exec(binned_query).fetchall()

    # Format the results
    formatted_results = []
    for row in results:
        start_date = row.bin
        end_date = calculate_end_date(start_date, freq)
        formatted_results.append({'start': start_date, 'end': end_date, 'value': int(row.value)})

    return PaginatedProjectCreditTotals(
        pagination=Pagination(
            total_entries=len(formatted_results),
            total_pages=1,
            next_page=None,
            current_page=current_page,
        ),
        data=formatted_results,
    )


@router.get('/projects_by_credit_totals', response_model=PaginatedBinnedCreditTotals)
@cache(namespace=CACHE_NAMESPACE)
async def get_projects_by_credit_totals(
    request: Request,
    credit_type: typing.Literal['issued', 'retired'] = Query('issued', description='Credit type'),
    project_filters: ProjectFilters = Depends(get_project_filters),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    bin_width: int | None = Query(None, description='Bin width'),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get aggregated project credit totals"""

    logger.info(f'📊 Generating projects by {credit_type} totals...: {request.url}')
    project_filters.type = expand_project_types(session, project_filters.type)

    query = select(Project)

    filters = build_filters(project_filters=project_filters)

    for attribute, values, operation, model in filters:
        query = apply_filters(
            statement=query,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.where(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    subquery = query.subquery()

    # Get min and max values for binning
    min_max_query = select(
        func.min(getattr(subquery.c, credit_type)).label('min_value'),
        func.max(getattr(subquery.c, credit_type)).label('max_value'),
    )
    min_max_result = session.exec(min_max_query).fetchone()
    min_value, max_value = min_max_result.min_value, min_max_result.max_value

    if min_value is None or max_value is None:
        logger.info('✅ No data to bin!')
        return PaginatedBinnedCreditTotals(
            pagination=Pagination(
                total_entries=0,
                total_pages=1,
                next_page=None,
                current_page=current_page,
            ),
            data=[],
        )

    # Generate bins
    bins = generate_dynamic_numeric_bins(
        min_value=min_value, max_value=max_value, bin_width=bin_width
    ).tolist()

    # Create a CASE statement for binning
    bin_case = case(
        *[
            (
                and_(
                    getattr(subquery.c, credit_type) >= bin_start,
                    getattr(subquery.c, credit_type) < bin_end,
                ),
                bin_start,
            )
            for bin_start, bin_end in zip(bins[:-1], bins[1:])
        ],
        else_=bins[-2],  # Use the last bin start for values >= the last bin start
    ).label('bin')

    # Count projects by bin and category
    binned_query = select(
        bin_case, subquery.c.category, func.count(subquery.c.project_id.distinct()).label('value')
    ).group_by(bin_case, subquery.c.category)

    # Execute the query
    results = session.exec(binned_query).fetchall()

    # Format the results
    formatted_results = []
    for row in results:
        bin_start = row.bin
        bin_index = bins.index(bin_start)
        bin_end = bins[bin_index + 1] if bin_index < len(bins) - 1 else None
        formatted_results.append(
            {'start': bin_start, 'end': bin_end, 'category': row.category, 'value': int(row.value)}
        )

    logger.info(f'✅ {len(formatted_results)} bins generated')

    return PaginatedBinnedCreditTotals(
        pagination=Pagination(
            total_entries=len(formatted_results),
            total_pages=1,
            next_page=None,
            current_page=current_page,
        ),
        data=formatted_results,
    )


@router.get('/projects_by_category', response_model=PaginatedProjectCounts)
@cache(namespace=CACHE_NAMESPACE)
async def get_projects_by_category(
    request: Request,
    project_filters: ProjectFilters = Depends(get_project_filters),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get project counts by category"""

    logger.info(f'Getting project count by category: {request.url}')
    project_filters.type = expand_project_types(session, project_filters.type)

    query = select(Project)

    filters = build_filters(project_filters=project_filters)

    for attribute, values, operation, model in filters:
        query = apply_filters(
            statement=query,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.where(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    subquery = query.subquery()

    # Count projects by category - no need to unnest anymore
    projects_count_query = select(
        subquery.c.category, func.count(subquery.c.project_id.distinct()).label('value')
    ).group_by(subquery.c.category)

    results = session.exec(projects_count_query).fetchall()

    formatted_results = [{'category': row.category, 'value': int(row.value)} for row in results]

    return PaginatedProjectCounts(
        data=formatted_results,
        pagination=Pagination(
            current_page=current_page, next_page=None, total_entries=len(results), total_pages=1
        ),
    )


@router.get('/credits_by_category', response_model=PaginatedCreditCounts)
@cache(namespace=CACHE_NAMESPACE)
async def get_credits_by_category(
    request: Request,
    project_filters: ProjectFilters = Depends(get_project_filters),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    beneficiary_filters: BeneficiaryFilters = Depends(get_beneficiary_filters),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get project counts by category"""

    logger.info(f'Getting project count by category: {request.url}')

    project_filters.type = expand_project_types(session, project_filters.type)

    # Base query without Credit join
    matching_projects = select(distinct(Project.project_id))

    filters = build_filters(project_filters=project_filters)

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        matching_projects = matching_projects.where(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    use_dynamic_retirement = False
    if beneficiary_filters.beneficiary_search:
        use_dynamic_retirement = True
        Credit_alias = aliased(Credit)
        matching_projects = matching_projects.outerjoin(
            Credit_alias, col(Project.project_id) == col(Credit_alias.project_id)
        )

        matching_projects = apply_beneficiary_search(
            statement=matching_projects,
            search_term=beneficiary_filters.beneficiary_search,
            search_fields=beneficiary_filters.beneficiary_search_fields,
            credit_model=Credit_alias,
            project_model=Project,
        )

    matching_projects_select = select(matching_projects.subquery())

    # Use the subquery to filter the main query
    query = select(Project).where(col(Project.project_id).in_(matching_projects_select))

    for attribute, values, operation, model in filters:
        query = apply_filters(
            statement=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Create the subquery with category
    subquery = query.subquery()

    if use_dynamic_retirement:
        # If beneficiary search is provided, compute retirement dynamically from transactions
        # Join with credits table to get transactions
        Credits_table = aliased(Credit)
        dynamic_data = (
            select(
                subquery.c.category,
                literal(0).label(
                    'issued'
                ),  # Return 0 for issued credits when using beneficiary search
                func.sum(
                    case(
                        (
                            Credits_table.transaction_type.ilike('%retirement%'),
                            Credits_table.quantity,
                        ),
                        else_=0,
                    )
                ).label('retired'),
            )
            .select_from(subquery)
            .outerjoin(Credits_table, subquery.c.project_id == Credits_table.project_id)
            .where(
                or_(
                    *[
                        getattr(Credits_table, field).ilike(
                            f'%{beneficiary_filters.beneficiary_search}%'
                        )
                        for field in beneficiary_filters.beneficiary_search_fields
                        if hasattr(Credits_table, field)
                    ]
                )
            )
            .group_by(subquery.c.category, subquery.c.issued)
        )

        statement = dynamic_data.subquery()

    else:
        # No need to unnest anymore, just select the columns directly
        statement = (
            select(
                subquery.c.category,
                subquery.c.issued,
                subquery.c.retired,
            )
            .select_from(subquery)
            .subquery()
        )

    # Group by category and sum issued and retired credits
    credits_query = select(
        statement.c.category,
        func.sum(statement.c.issued).label('issued'),
        func.sum(statement.c.retired).label('retired'),
    ).group_by(statement.c.category)

    # Execute the query
    results = session.exec(credits_query).fetchall()

    # Format the results
    formatted_results = [
        {'category': row.category, 'issued': int(row.issued), 'retired': int(row.retired)}
        for row in results
    ]

    return PaginatedCreditCounts(
        data=formatted_results,
        pagination=Pagination(
            current_page=current_page,
            next_page=None,
            total_entries=len(formatted_results),
            total_pages=1,
        ),
    )
