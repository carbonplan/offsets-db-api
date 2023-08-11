import datetime
import typing

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, and_, case, func

from ..database import get_session
from ..logging import get_logger
from ..models import Credit, Project, ProjectBinnedIssuanceTotals, ProjectBinnedRegistration
from ..query_helpers import apply_filters
from ..schemas import Registries

router = APIRouter()
logger = get_logger()


def calculate_end_date(start_date, freq):
    """Calculate the end date based on the start date and frequency."""
    if freq == 'D':
        return start_date + pd.DateOffset(days=1)
    elif freq == 'W':
        return start_date + pd.DateOffset(weeks=1)
    elif freq == 'M':
        return start_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    else:  # freq == 'Y'
        return start_date + pd.DateOffset(years=1) - pd.DateOffset(days=1)


def generate_date_bins(*, min_value, max_value, freq: typing.Literal['D', 'W', 'M', 'Y']):
    """Generate date bins with the specified frequency."""
    start_of_period = pd.Timestamp(min_value)
    end_of_period = pd.Timestamp(max_value)
    if freq == 'M':
        end_of_period = (
            end_of_period.replace(day=1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)
        )
    elif freq == 'Y':
        start_of_period = start_of_period.replace(month=1, day=1)  # Start of the year
        end_of_period = end_of_period.replace(month=12, day=31)

    frequency_mapping = {'Y': 'AS', 'M': 'MS', 'W': 'W', 'D': 'D'}

    logger.info(
        f'📅 Binning by date with {freq} frequency, start_period: {start_of_period}, end_of_period: {end_of_period}'
    )

    date_bins = pd.date_range(
        start=start_of_period, end=end_of_period, freq=frequency_mapping[freq]
    )

    # Ensure the last date is included
    if len(date_bins) == 0 or date_bins[-1] != end_of_period:
        date_bins = date_bins.append(pd.DatetimeIndex([end_of_period]))

    logger.info(f'📅 Binning by date with {len(date_bins)} bins...: {date_bins}')
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
        binned_attribute, Project.category, func.count(Project.project_id).label('value')
    )
    binned_results = query.group_by('bin', Project.category).all()

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


def credits_by_issuance_date(*, query, freq='Y'):
    """Generate binned data based on the issuance date."""
    logger.info('📊 Generating binned data based on issuance date...')

    # Extract the minimum and maximum transaction_date
    min_date, max_date = query.with_entities(
        func.min(Credit.transaction_date), func.max(Credit.transaction_date)
    ).one()

    if min_date is None or max_date is None:
        logger.info('✅ No data to bin!')
        return []

    # Generate date bins based on the frequency
    date_bins = generate_date_bins(min_value=min_date, max_value=max_date, freq=freq)

    # Create conditions for binning
    conditions = [
        (
            and_(
                Credit.transaction_date >= date_bins[i], Credit.transaction_date < date_bins[i + 1]
            ),
            str(date_bins[i].date()),
        )
        for i in range(len(date_bins) - 1)
    ]
    conditions.append((Credit.transaction_date.is_(None), 'null'))

    # Define the binned attribute
    binned_attribute = case(conditions, else_='other').label('bin')

    # Query and format the results
    query = query.with_entities(
        binned_attribute, Project.category, func.sum(Credit.quantity).label('value')
    ).group_by('bin', Project.category)

    binned_results = query.all()

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
    session: Session = Depends(get_session),
):
    """Get aggregated project registration data"""
    logger.info(f'Getting project registration data: {request.url}')

    query = session.query(Project)

    # Apply filters
    filterable_attributes = [
        ('registry', registry, 'ilike'),
        ('country', country, 'ilike'),
        ('protocol', protocol, 'ilike'),
        ('category', category, 'ilike'),
    ]

    for attribute, values, operation in filterable_attributes:
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

    return get_binned_data(binning_attribute='registered_at', query=query, freq=freq)


@router.get('/credits_by_issuance_date', response_model=list[dict])
def get_credits_by_issuance_date(
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
    session: Session = Depends(get_session),
):
    """Get aggregated project registration data"""
    logger.info(f'Getting project registration data: {request.url}')

    # join Credit with Project on project_id
    query = session.query(Credit).join(Project, Credit.project_id == Project.project_id)

    # Apply filters
    filterable_attributes = [
        ('registry', registry, 'ilike'),
        ('country', country, 'ilike'),
        ('protocol', protocol, 'ilike'),
        ('category', category, 'ilike'),
    ]

    for attribute, values, operation in filterable_attributes:
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

    return credits_by_issuance_date(query=query, freq=freq)
