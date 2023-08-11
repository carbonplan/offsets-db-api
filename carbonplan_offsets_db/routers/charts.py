import datetime
import typing

import pandas as pd
from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, and_, case, func, or_, text

from ..database import get_session
from ..logging import get_logger
from ..models import Project, ProjectBinnedIssuanceTotals, ProjectBinnedRegistration
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
        return start_date + pd.DateOffset(months=1) + pd.offsets.MonthEnd(0)
    else:  # freq == 'Y'
        return start_date + pd.DateOffset(years=1) + pd.offsets.YearEnd(0)


def generate_date_bins(*, min_value, max_value, freq: typing.Literal['D', 'W', 'M', 'Y']):
    """Generate date bins with the specified frequency."""
    end_of_period = pd.Timestamp(max_value)
    if freq == 'M':
        end_of_period = (
            end_of_period.replace(day=1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)
        )
    elif freq == 'Y':
        end_of_period = end_of_period.replace(month=12, day=31)

    date_bins = pd.date_range(start=min_value, end=max_value, freq=freq)

    # Ensure the last date is included
    if len(date_bins) == 0 or date_bins[-1] != end_of_period:
        date_bins = date_bins.append(pd.DatetimeIndex([end_of_period]))

    logger.info(f'📅 Binning by date with {len(date_bins)} bins...')
    return date_bins


def get_binned_data(*, query, binning_attribute, freq='Y'):
    """Generate binned data based on the given attribute and frequency."""
    logger.info(f'📊 Generating binned data based on {binning_attribute}...')
    attribute = getattr(Project, binning_attribute)
    min_value, max_value = query.with_entities(func.min(attribute), func.max(attribute)).one()

    date_bins = generate_date_bins(min_value=min_value, max_value=max_value, freq=freq)

    # Create conditions for each bin
    conditions = [
        (
            and_(attribute >= date_bins[i], attribute < date_bins[i + 1]),
            str(date_bins[i].date()),
        )
        for i in range(len(date_bins) - 1)
    ]

    # Define the binned attribute
    if conditions:
        binned_attribute = case(conditions, else_='other').label('bin')
    elif len(date_bins) == 1:
        binned_attribute = func.concat(date_bins[0].date()).label(
            'bin'
        )  # Use concat to return a string literal
    else:
        binned_attribute = text('other')  # Explicitly declare the text literal

    # Query and format the results
    query = query.with_entities(
        binned_attribute, Project.category, func.count(Project.project_id).label('value')
    )
    binned_results = query.group_by('bin', Project.category).all()

    formatted_results = []
    for bin_label, category, value in binned_results:
        start_date = pd.Timestamp(bin_label) if bin_label != 'other' else None
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

    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    return get_binned_data(binning_attribute='registered_at', query=query, freq=freq)


@router.get('/issuance_totals', response_model=list[ProjectBinnedIssuanceTotals])
def get_issuance_totals(
    request: Request,
    num_bins: int = Query(15, description='The number of bins'),
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

    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    # Fetch filtered projects for binning
    filtered_projects = query.all()
    # Check if the filtered projects list is empty
    if not filtered_projects:
        logger.warning('⚠️ No projects found matching the filtering criteria!')
        return []

    return get_binned_data(
        session=session,
        num_bins=num_bins,
        binning_attribute='issued',
        projects=filtered_projects,
    )
