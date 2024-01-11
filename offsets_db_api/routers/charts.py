import datetime
import typing

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, and_, case, col, func, or_

from ..database import get_engine, get_session
from ..logging import get_logger
from ..models import (
    Credit,
    PaginatedBinnedCreditTotals,
    PaginatedBinnedValues,
    PaginatedCreditCounts,
    PaginatedProjectCounts,
    PaginatedProjectCreditTotals,
    Project,
)
from ..query_helpers import apply_filters
from ..schemas import Pagination, Registries
from ..security import check_api_key
from ..settings import get_settings

router = APIRouter()
logger = get_logger()


def filter_valid_projects(df: pd.DataFrame, categories: list | None = None) -> pd.DataFrame:
    if categories is None:
        return df
    # Filter the dataframe to include only rows with the specified categories
    valid_projects = df[df['category'].isin(categories)]
    return valid_projects


def projects_by_category(
    *, df: pd.DataFrame, categories: list | None = None
) -> list[dict[str, int]]:
    valid_projects = filter_valid_projects(df, categories)
    counts = valid_projects.groupby('category').count()['project_id']
    return [{'category': category, 'value': count} for category, count in counts.items()]


def credits_by_category(
    *, df: pd.DataFrame, categories: list | None = None
) -> list[dict[str, int]]:
    valid_projects = filter_valid_projects(df, categories)
    credits = (
        valid_projects.groupby('category').agg({'issued': 'sum', 'retired': 'sum'}).reset_index()
    )
    return [
        {'category': row['category'], 'issued': row['issued'], 'retired': row['retired']}
        for _, row in credits.iterrows()
    ]


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


def get_binned_data(*, query, binning_attribute, freq='Y'):
    """Generate binned data based on the given attribute and frequency."""
    logger.info(f'📊 Generating binned data based on {binning_attribute}...')
    attribute = getattr(Project, binning_attribute)
    min_value, max_value = query.with_entities(func.min(attribute), func.max(attribute)).one()

    if min_value is None or max_value is None:
        logger.info('✅ No data to bin!')
        return []

    date_bins = generate_date_bins(min_value=min_value, max_value=max_value, freq=freq)

    conditions = []
    # Handle the case of exactly one non-null date bin
    if len(date_bins) == 1:
        conditions.append((attribute.isnot(None), func.concat(date_bins[0].date())))

    # Handle the case of multiple non-null date bins
    else:
        conditions.extend(
            [
                (
                    and_(attribute >= date_bins[i], attribute < date_bins[i + 1]),
                    str(date_bins[i].date()),
                )
                for i in range(len(date_bins) - 1)
            ]
        )
    # Add condition for null registration dates
    conditions.append((attribute.is_(None), 'null'))

    # Define the binned attribute
    binned_attribute = case(conditions, else_='other').label('bin')

    # Query and format the results
    query = query.with_entities(
        binned_attribute,
        func.unnest(Project.category).label('category'),
        func.count(Project.project_id).label('value'),
    )
    binned_results = query.group_by('bin', 'category').all()

    formatted_results = []
    for bin_label, category, value in binned_results:
        start_date = pd.Timestamp(bin_label) if bin_label not in ['other', 'null'] else None
        end_date = calculate_end_date(start_date, freq).date() if start_date else None
        formatted_results.append(
            dict(start=start_date, end=end_date, category=category, value=value)
        )

    logger.info('✅ Binned data generated successfully!')

    return formatted_results


def projects_by_credit_totals(
    *, df: pd.DataFrame, credit_type: str, bin_width=None, categories=None
) -> list[dict[str, typing.Any]]:
    """Generate binned data based on the given attribute and frequency."""
    logger.info(f'📊 Generating binned data based on {credit_type}...')
    valid_df = filter_valid_projects(df, categories=categories)
    min_value, max_value = valid_df[credit_type].agg(['min', 'max'])

    if pd.isna(min_value) or pd.isna(max_value):
        logger.info('✅ No data to bin!')
        return []

    bins = generate_dynamic_numeric_bins(
        min_value=min_value, max_value=max_value, bin_width=bin_width
    ).tolist()
    valid_df['bin'] = pd.cut(valid_df[credit_type], bins=bins, labels=bins[:-1], right=False)
    valid_df['bin'] = valid_df['bin'].astype(str)
    grouped = valid_df.groupby(['bin', 'category'])['project_id'].count().reset_index()
    formatted_results = []
    for _, row in grouped.iterrows():
        bin_label = row['bin']
        category = row['category']
        value = row['project_id']
        start_value = int(bin_label)
        index = bins.index(start_value)
        end_value = bins[index + 1]
        formatted_results.append(
            dict(start=start_value, end=end_value, category=category, value=value)
        )
    logger.info(f'✅ {len(formatted_results)} bins generated')

    return formatted_results


def single_project_credits_by_transaction_date(
    *, df: pd.DataFrame, freq: typing.Literal['D', 'W', 'M', 'Y'] | None = None
) -> list[dict[str, typing.Any]]:
    min_date, max_date = df.transaction_date.agg(['min', 'max'])
    if pd.isna(min_date) or pd.isna(max_date):
        logger.info('✅ No data to bin!')
        return []

    if freq is None:
        date_diff = max_date - min_date
        if date_diff < datetime.timedelta(days=7):
            freq = 'D'
        elif date_diff < datetime.timedelta(
            days=30
        ):  # Approximating a month to 30 days for simplicity
            freq = 'W'
        elif date_diff < datetime.timedelta(
            days=365
        ):  # Approximating a year to 365 days for simplicity
            freq = 'M'
        else:
            freq = 'Y'

        # Check if all events fall within the same year or month
        if min_date.year == max_date.year:
            freq = 'Y'
            if min_date.month == max_date.month:
                freq = 'M'

    date_bins = generate_date_bins(min_value=min_date, max_value=max_date, freq=freq)

    # Binning logic
    df['bin'] = pd.cut(df['transaction_date'], bins=date_bins, labels=date_bins[:-1], right=False)
    df['bin'] = df['bin'].astype(str)
    grouped = df.groupby(['bin'])['quantity'].sum().reset_index()
    formatted_results = []
    for _, row in grouped.iterrows():
        bin_label = row['bin']
        value = row['quantity']

        start_date = pd.Timestamp(bin_label).date() if bin_label else None
        end_date = calculate_end_date(start_date, freq).date() if start_date else None

        formatted_results.append(dict(start=start_date, end=end_date, value=value))
    return formatted_results


def credits_by_transaction_date(
    *,
    df: pd.DataFrame,
    freq: typing.Literal['D', 'W', 'M', 'Y'] = 'Y',
    num_bins: int | None = None,
    categories: list[str] | None = None,
) -> list[dict[str, typing.Any]]:
    """
    Get credits by transaction date.
    """
    valid_df = filter_valid_projects(df, categories=categories)

    min_date, max_date = valid_df.transaction_date.agg(['min', 'max'])

    if pd.isna(min_date) or pd.isna(max_date):
        logger.info('✅ No data to bin!')
        return []
    if num_bins:
        date_bins = generate_date_bins(
            min_value=min_date, max_value=max_date, num_bins=num_bins
        )  # Assuming this function returns a list of date ranges
    else:
        date_bins = generate_date_bins(min_value=min_date, max_value=max_date, freq=freq)

    # Binning logic
    valid_df['bin'] = pd.cut(
        valid_df['transaction_date'], bins=date_bins, labels=date_bins[:-1], right=False
    )
    valid_df['bin'] = valid_df['bin'].astype(str)

    # Aggregate the data
    grouped = valid_df.groupby(['bin', 'category'])['quantity'].sum().reset_index()

    # Formatting the results
    formatted_results = []
    for _, row in grouped.iterrows():
        bin_label = row['bin']
        category = row['category']
        value = row['quantity']

        start_date = pd.Timestamp(bin_label).date() if bin_label else None
        end_date = calculate_end_date(start_date, freq).date() if start_date else None

        formatted_results.append(
            dict(start=start_date, end=end_date, category=category, value=value)
        )
    return formatted_results


@router.get('/projects_by_listing_date', response_model=PaginatedBinnedValues)
def get_projects_by_listing_date(
    request: Request,
    freq: typing.Literal['D', 'W', 'M', 'Y'] = Query('Y', description='Frequency of bins'),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    listed_at_from: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    listed_at_to: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
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

    query = session.query(Project)

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('listed_at', listed_at_from, '>=', Project),
        ('listed_at', listed_at_to, '<=', Project),
        ('issued', issued_min, '>=', Project),
        ('issued', issued_max, '<=', Project),
        ('retired', retired_min, '>=', Project),
        ('retired', retired_max, '<=', Project),
    ]

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    results = get_binned_data(binning_attribute='listed_at', query=query, freq=freq)
    total_entries = len(results)
    total_pages = 1
    next_page = None

    return PaginatedBinnedValues(
        pagination=Pagination(
            total_entries=total_entries,
            total_pages=total_pages,
            next_page=next_page,
            current_page=current_page,
        ),
        data=results,
    )


@router.get('/credits_by_transaction_date', response_model=PaginatedBinnedValues)
def get_credits_by_transaction_date(
    request: Request,
    freq: typing.Literal['D', 'W', 'M', 'Y'] = Query('Y', description='Frequency of bins'),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    vintage: list[int] | None = Query(None, description='Vintage'),
    transaction_date_from: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    transaction_date_to: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
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

    # join Credit with Project on project_id
    query = session.query(Credit, Project.category).join(
        Project, Credit.project_id == Project.project_id, isouter=True
    )

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('transaction_type', transaction_type, 'ilike', Credit),
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('vintage', vintage, '==', Credit),
        ('transaction_date', transaction_date_from, '>=', Credit),
        ('transaction_date', transaction_date_to, '<=', Credit),
    ]

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)

    logger.info(f'Query statement: {query.statement}')

    df = pd.read_sql_query(query.statement, engine).explode('category')
    logger.info(f'Sample of the dataframe with size: {df.shape}\n{df.head()}')
    # fix the data types
    df = df.astype({'transaction_date': 'datetime64[ns]'})
    results = credits_by_transaction_date(df=df, freq=freq, categories=category)

    total_entries = len(results)
    total_pages = 1
    next_page = None
    return PaginatedBinnedValues(
        pagination=Pagination(
            total_entries=total_entries,
            total_pages=total_pages,
            next_page=next_page,
            current_page=current_page,
        ),
        data=results,
    )


@router.get(
    '/credits_by_transaction_date/{project_id}', response_model=PaginatedProjectCreditTotals
)
def get_credits_by_project_id(
    request: Request,
    project_id: str,
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    vintage: list[int] | None = Query(None, description='Vintage'),
    transaction_date_from: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    transaction_date_to: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    freq: typing.Literal['D', 'W', 'M', 'Y'] = Query('Y', description='Frequency of bins'),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """Get aggregated credit transaction data"""
    logger.info(f'Getting credit transaction data: {request.url}')
    # Join Credit with Project and filter by project_id
    query = (
        session.query(Credit, Project.category, Project.listed_at)
        .join(Project, Credit.project_id == Project.project_id)
        .filter(Project.project_id == project_id)
    )

    filters = [
        ('transaction_type', transaction_type, 'ilike', Credit),
        ('transaction_date', transaction_date_from, '>=', Credit),
        ('transaction_date', transaction_date_to, '<=', Credit),
        ('vintage', vintage, '==', Credit),
    ]

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)

    logger.info(f'Query statement: {query.statement}')

    df = pd.read_sql_query(query.statement, engine)
    # fix the data types
    df = df.astype({'transaction_date': 'datetime64[ns]'})
    results = single_project_credits_by_transaction_date(df=df, freq=freq)

    total_entries = len(results)
    total_pages = 1
    next_page = None
    return PaginatedProjectCreditTotals(
        pagination=Pagination(
            total_entries=total_entries,
            total_pages=total_pages,
            next_page=next_page,
            current_page=current_page,
        ),
        data=results,
    )


@router.get('/projects_by_credit_totals')
def get_projects_by_credit_totals(
    request: Request,
    credit_type: typing.Literal['issued', 'retired'] = Query('issued', description='Credit type'),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    listed_at_from: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    listed_at_to: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    started_at_from: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    started_at_to: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
    search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    bin_width: int | None = Query(None, description='Bin width'),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    # authorized_user: bool = Depends(check_api_key),
):
    """Get aggregated project credit totals"""
    logger.info(f'📊 Generating projects by {credit_type} totals...: {request.url}')

    query = session.query(Project)

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('listed_at', listed_at_from, '>=', Project),
        ('listed_at', listed_at_to, '<=', Project),
        ('started_at', started_at_from, '>=', Project),
        ('started_at', started_at_to, '<=', Project),
        ('issued', issued_min, '>=', Project),
        ('issued', issued_max, '<=', Project),
        ('retired', retired_min, '>=', Project),
        ('retired', retired_max, '<=', Project),
    ]

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)
    logger.info(f'Query statement: {query.statement}')

    df = pd.read_sql_query(query.statement, engine).explode('category')
    logger.info(f'Sample of the dataframe with size: {df.shape}\n{df.head()}')
    results = projects_by_credit_totals(df=df, credit_type=credit_type, bin_width=bin_width)

    total_entries = len(results)
    total_pages = 1
    next_page = None
    return PaginatedBinnedCreditTotals(
        pagination=Pagination(
            total_entries=total_entries,
            total_pages=total_pages,
            next_page=next_page,
            current_page=current_page,
        ),
        data=results,
    )


@router.get('/projects_by_category', response_model=PaginatedProjectCounts)
def get_projects_by_category(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    listed_at_from: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    listed_at_to: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
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

    query = session.query(Project)

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('listed_at', listed_at_from, '>=', Project),
        ('listed_at', listed_at_to, '<=', Project),
        ('issued', issued_min, '>=', Project),
        ('issued', issued_max, '<=', Project),
        ('retired', retired_min, '>=', Project),
        ('retired', retired_max, '<=', Project),
    ]

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)

    df = pd.read_sql_query(query.statement, engine).explode('category')
    logger.info(f'Sample of the dataframe with size: {df.shape}\n{df.head()}')
    results = projects_by_category(df=df, categories=category)

    return PaginatedProjectCounts(
        data=results,
        pagination=Pagination(
            current_page=current_page, next_page=None, total_entries=len(results), total_pages=1
        ),
    )


@router.get('/credits_by_category', response_model=PaginatedCreditCounts)
def get_credits_by_category(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    listed_at_from: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    listed_at_to: datetime.date | datetime.datetime | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
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

    query = session.query(Project)

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('listed_at', listed_at_from, '>=', Project),
        ('listed_at', listed_at_to, '<=', Project),
        ('issued', issued_min, '>=', Project),
        ('issued', issued_max, '<=', Project),
        ('retired', retired_min, '>=', Project),
        ('retired', retired_max, '<=', Project),
    ]

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(
                col(Project.project_id).ilike(search_pattern),
                col(Project.name).ilike(search_pattern),
            )
        )

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)

    df = pd.read_sql_query(query.statement, engine).explode('category')
    logger.info(f'Sample of the dataframe with size: {df.shape}\n{df.head()}')

    results = credits_by_category(df=df, categories=category)

    return PaginatedCreditCounts(
        data=results,
        pagination=Pagination(
            current_page=current_page, next_page=None, total_entries=len(results), total_pages=1
        ),
    )
