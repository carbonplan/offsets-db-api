import datetime
import typing

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, and_, case, func, or_

from ..database import get_session
from ..logging import get_logger
from ..models import Credit, Project, ProjectBinnedIssuanceTotals, ProjectBinnedRegistration
from ..query_helpers import apply_filters
from ..schemas import Registries

router = APIRouter()
logger = get_logger()


def remove_duplicates(binned_results):
    seen = set()
    unique_results = []

    for result in binned_results:
        # Convert the result to a tuple and add it to the set
        result_tuple = (result['bin'], result['category'], result['value'])

        # Check if the tuple is in the set (i.e., if it's a duplicate)
        if result_tuple not in seen:
            seen.add(result_tuple)
            unique_results.append(result)

    return unique_results


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
    min_value, max_value, freq: typing.Literal['D', 'W', 'M', 'Y'], num_bins: int = None
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

    if num_bins:
        # Generate 'num_bins + 1' points and slice off the last one to get exactly 'num_bins' start points
        return pd.date_range(start=min_value, end=max_value, periods=num_bins + 1)[:-1]
    frequency_mapping = {'Y': 'AS', 'M': 'MS', 'W': 'W', 'D': 'D'}
    min_value, max_value = pd.Timestamp(min_value), pd.Timestamp(max_value)
    date_bins = pd.date_range(
        start=pd.Timestamp(min_value).replace(month=1, day=1),
        end=pd.Timestamp(max_value).replace(month=12, day=31),
        freq=frequency_mapping[freq],
        normalize=True,
    )

    # Ensure the last date is included
    if len(date_bins) == 0 or date_bins[-1] < max_value:
        date_bins = date_bins.append(pd.DatetimeIndex([max_value.replace(month=12, day=31)]))

    return date_bins


def generate_dynamic_numeric_bins(*, min_value, max_value, bin_width=None):
    """Generate numeric bins with dynamically adjusted bin width."""
    # Check for edge cases where min and max are the same
    if min_value == max_value:
        return np.array([min_value])

    if bin_width is None:
        # Calculate the range and order of magnitude
        value_range = max_value - min_value
        order_of_magnitude = int(np.floor(np.log10(value_range)))

        # Determine the bin width based on the order of magnitude
        if order_of_magnitude < 2:
            bin_width = 10  # Tens for very small ranges
        elif order_of_magnitude < 3:
            bin_width = 100  # Hundreds for small ranges
        elif order_of_magnitude < 4:
            bin_width = 1000  # Thousands for lower moderate ranges
        elif order_of_magnitude < 5:
            bin_width = 10000  # Ten thousands for upper moderate ranges
        elif order_of_magnitude < 6:
            bin_width = 100000  # Hundred thousands for large ranges
        else:
            bin_width = 1000000  # Millions for very large ranges

    # Generate evenly spaced values using the determined bin width
    numeric_bins = np.arange(min_value, max_value + bin_width, bin_width)

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
            ProjectBinnedRegistration(
                start=start_date, end=end_date, category=category, value=value
            )
        )

    logger.info('✅ Binned data generated successfully!')
    return formatted_results


def get_binned_numeric_data(*, query, binning_attribute):
    """Generate binned data based on the given numeric attribute."""
    logger.info(f'📊 Generating binned data based on {binning_attribute}...')
    attribute = getattr(Credit, binning_attribute)
    min_value, max_value = query.with_entities(func.min(attribute), func.max(attribute)).one()

    if min_value is None or max_value is None:
        logger.info('✅ No data to bin!')
        return []

    numeric_bins = generate_dynamic_numeric_bins(min_value=min_value, max_value=max_value)

    conditions = []
    # Handle the case of exactly one non-null bin
    if len(numeric_bins) == 1:
        conditions.append((attribute.isnot(None), str(int(numeric_bins[0]))))

    # Handle the case of multiple non-null bins
    else:
        conditions.extend(
            [
                (
                    and_(attribute >= int(numeric_bins[i]), attribute < int(numeric_bins[i + 1])),
                    str(int(numeric_bins[i])),
                )
                for i in range(len(numeric_bins) - 1)
            ]
        )
    # Add condition for null attributes
    conditions.append((attribute.is_(None), 'null'))

    # Define the binned attribute
    binned_attribute = case(conditions, else_='other').label('bin')

    # Query and format the results
    query = query.with_entities(
        binned_attribute, Project.category, func.sum(Credit.quantity).label('value')
    )
    binned_results = query.group_by('bin', Project.category).all()

    formatted_results = []
    for bin_label, category, value in binned_results:
        start_value = float(bin_label) if bin_label not in ['other', 'null'] else None
        end_value = start_value + 1 if start_value else None
        formatted_results.append(
            ProjectBinnedIssuanceTotals(
                start=start_value, end=end_value, category=category, value=value
            )
        )

    logger.info('✅ Binned data generated successfully!')
    return formatted_results


def credits_by_transaction_date(
    *,
    query,
    min_date,
    max_date,
    freq: typing.Literal['D', 'W', 'M', 'Y'] = 'Y',
    num_bins: int = None,
):
    """Generate binned data based on the transaction date."""
    logger.info('📊 Generating binned data based on transaction date...')

    if min_date is None or max_date is None:
        logger.info('✅ No data to bin!')
        return []

    if num_bins:
        date_bins = generate_date_bins(
            min_value=min_date, max_value=max_date, freq=freq, num_bins=num_bins
        )
        logger.info(
            f'📅 Binning by date with {date_bins} bins... min_value: {min_date}, max_value={max_date}, num_bins={num_bins}'
        )

    else:
        # Generate date bins based on the frequency
        date_bins = generate_date_bins(min_value=min_date, max_value=max_date, freq=freq)
        logger.info(
            f'📅 Binning by date with {date_bins} bins... min_value: {min_date}, max_value={max_date}, freq={freq}'
        )

    # Create conditions for binning
    conditions = []
    # Handle the case of exactly one non-null date bin
    if len(date_bins) == 1:
        conditions.append((Credit.transaction_date.isnot(None), str(date_bins[0].date())))
    else:
        conditions.extend(
            [
                (
                    and_(
                        Credit.transaction_date >= date_bins[i],
                        Credit.transaction_date < date_bins[i + 1],
                    ),
                    str(date_bins[i].date()),
                )
                for i in range(len(date_bins) - 1)
            ]
        )
    conditions.append((Credit.transaction_date.is_(None), 'null'))

    # Define the binned attribute
    binned_attribute = case(conditions, else_='other').label('bin')

    # Query and format the results
    query = query.with_entities(
        binned_attribute,
        func.unnest(Project.category).label('category'),
        func.sum(Credit.quantity).label('value'),
    ).group_by('bin', 'category')

    binned_results = query.all()
    binned_results = remove_duplicates(binned_results)

    formatted_results = []
    for bin_label, category, value in binned_results:
        start_date = pd.Timestamp(bin_label) if bin_label not in ['other', 'null'] else None
        end_date = calculate_end_date(start_date, freq).date() if start_date else None
        formatted_results.append(
            ProjectBinnedRegistration(
                start=start_date, end=end_date, category=category, value=value
            )
        )

    logger.info('✅ Binned data generated successfully!')
    return formatted_results


@router.get('/projects_by_registration_date', response_model=list[ProjectBinnedRegistration])
def get_projects_by_registration_date(
    request: Request,
    freq: typing.Literal['D', 'W', 'M', 'Y'] = Query('Y', description='Frequency of bins'),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_arb: bool | None = Query(None, description='Whether project is an ARB project'),
    registered_at_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    registered_at_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    started_at_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    started_at_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
    search: str
    | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    session: Session = Depends(get_session),
):
    """Get aggregated project registration data"""
    logger.info(f'Getting project registration data: {request.url}')

    query = session.query(Project)

    # Apply filters
    filterable_attributes = [('registry', registry, 'ilike'), ('country', country, 'ilike')]

    for attribute, values, operation in filterable_attributes:
        query = apply_filters(
            query=query, model=Project, attribute=attribute, values=values, operation=operation
        )

    list_attributes = [('protocol', protocol, 'ANY'), ('category', category, 'ANY')]
    for attribute, values, operation in list_attributes:
        query = apply_filters(
            query=query, model=Project, attribute=attribute, values=values, operation=operation
        )

    other_filters = [
        ('is_arb', is_arb, '=='),
        ('registered_at', registered_at_from, '>='),
        ('registered_at', registered_at_to, '<='),
        ('started_at', started_at_from, '>='),
        ('started_at', started_at_to, '<='),
        ('issued', issued_min, '>='),
        ('issued', issued_max, '<='),
        ('retired', retired_min, '>='),
        ('retired', retired_max, '<='),
    ]

    for attribute, values, operation in other_filters:
        query = apply_filters(
            query=query, model=Project, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    return get_binned_data(binning_attribute='registered_at', query=query, freq=freq)


@router.get('/credits_by_transaction_date', response_model=list[dict])
def get_credits_by_transaction_date(
    request: Request,
    freq: typing.Literal['D', 'W', 'M', 'Y'] = Query('Y', description='Frequency of bins'),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
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
    session: Session = Depends(get_session),
):
    """Get aggregated credit transaction data"""
    logger.info(f'Getting credit transaction data: {request.url}')

    # join Credit with Project on project_id
    query = session.query(Credit).join(
        Project, Credit.project_id == Project.project_id, isouter=True
    )

    # Filters applying 'ilike' operation
    ilike_filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('transaction_type', transaction_type, 'ilike', Credit),
    ]

    for attribute, values, operation, model in ilike_filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    list_attributes = [
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
    ]
    for attribute, values, operation, model in list_attributes:
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

    # Extract the minimum and maximum transaction_date
    min_date, max_date = query.with_entities(
        func.min(Credit.transaction_date), func.max(Credit.transaction_date)
    ).one()
    return credits_by_transaction_date(query=query, freq=freq, min_date=min_date, max_date=max_date)


@router.get('/credits_by_transaction_date/{project_id}', response_model=list[dict])
def get_credits_by_project_id(
    project_id: str,
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    vintage: list[int] | None = Query(None, description='Vintage'),
    transaction_date_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    transaction_date_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    num_bins: int = Query(20, description='Number of bins'),
    session: Session = Depends(get_session),
):
    # Join Credit with Project and filter by project_id
    query = (
        session.query(Credit)
        .join(Project, Credit.project_id == Project.project_id)
        .filter(Project.project_id == project_id)
    )

    # Filters applying 'ilike' operation
    ilike_filters = [
        ('transaction_type', transaction_type, 'ilike', Credit),
    ]

    for attribute, values, operation, model in ilike_filters:
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

    # Filter applying '==' operation
    equal_filters = [('vintage', vintage, '==', Credit)]

    for attribute, values, operation, model in equal_filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )
    # TODO: revisit this once we have reliable `listed_at`
    # ref: https://github.com/carbonplan/offsets-db/issues/31#issuecomment-1707434158
    # min_date is between project.registered_at and min(credit.transaction_date)
    # Query to get project.registered_at
    query1 = query.with_entities(Project.registered_at)
    project_registered_at = query1.first()[0] if query1.first() else None

    # Query to get min(Credit.transaction_date)
    query2 = session.query(func.min(Credit.transaction_date)).filter(
        Credit.project_id == project_id
    )
    min_transaction_date = query2.first()[0] if query2.first() else None

    # Find the minimum date between project_registered_at and min_transaction_date
    if project_registered_at is None and min_transaction_date is None:
        min_date = datetime.datetime.strptime('1990-01-01', '%Y-%m-%d').date()
    elif project_registered_at and min_transaction_date:
        min_date = min(project_registered_at, min_transaction_date)
    elif project_registered_at is None:
        min_date = min_transaction_date
    else:
        min_date = project_registered_at

    # Use today's date if the project hasn't wound down yet
    max_date = datetime.date.today()

    return credits_by_transaction_date(
        query=query, min_date=min_date, max_date=max_date, freq='D', num_bins=num_bins
    )
