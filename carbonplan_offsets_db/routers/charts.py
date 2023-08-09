import datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, and_, case, func, or_

from ..database import get_session
from ..logging import get_logger
from ..models import Project, ProjectBinnedRegistration
from ..query_helpers import apply_filters
from ..schemas import Registries

router = APIRouter()
logger = get_logger()


def get_binned_data(*, session, num_bins, projects=None):
    """
    This function bins the projects based on their registration date and groups them by category.

    Parameters
    ----------
    session: Session
        SQLAlchemy session for querying the database.
    num_bins: int,
        Number of bins to divide the registration dates into.
    projects: list, optional
        List of projects to be binned. If not provided, the function will query the entire Project table.

    Returns
    -------
    binned_results: list
        A list of tuples, each containing the bin label, category, and count of projects.
    """

    logger.info('📊 Generating binned data...')
    if projects:
        # Extract dates from provided projects, filtering out None values
        registration_dates = [
            project.registered_at for project in projects if project.registered_at is not None
        ]
        if not registration_dates:
            logger.error('❌ No valid registration dates found!')
            raise ValueError('Provided projects have no valid registration dates.')
        min_date = min(registration_dates)
        max_date = max(registration_dates)
    else:
        # Determine the earliest and latest registration dates in the database.
        min_date, max_date = session.query(
            func.min(Project.registered_at), func.max(Project.registered_at)
        ).one()

    # Calculate the width of each bin by dividing the total date range by the number of bins.
    bin_width = (max_date - min_date) / num_bins

    # Create conditions for each bin. Each condition checks if a project's registration date
    # falls within the range defined by a bin's start and end dates. Also, assign a label for each bin.
    conditions = [
        (
            and_(
                Project.registered_at >= min_date + i * bin_width,
                Project.registered_at < min_date + (i + 1) * bin_width,
            ),
            f'{(min_date + i*bin_width).year}-{(min_date + (i+1)*bin_width).year}',
        )
        for i in range(num_bins)
    ]

    # Using the conditions, generate a CASE statement to assign a bin label to each project.
    binned_date = case(conditions, else_='other').label('bin')

    # Execute the main query, grouping projects by bin and category, and counting the number of projects in each group.
    binned_results = (
        session.query(binned_date, Project.category, func.count(Project.id).label('count'))
        .group_by('bin', Project.category)
        .all()
    )

    # Validate that the sum of counts from the binned results matches the total number of projects in the database.
    total_projects = session.query(Project).count()
    total_binned_counts = sum(result[2] for result in binned_results)
    if total_projects != total_binned_counts:
        logger.error('❌ Mismatch in total counts!')
        raise ValueError(
            f"Total projects ({total_projects}) doesn't match sum of binned counts ({total_binned_counts})."
        )

    # Reformat results to a more concise representation.
    formatted_results = []
    for bin_label, category, count in binned_results:
        start, end = (
            (int(part) for part in bin_label.split('-')) if '-' in bin_label else (None, None)
        )
        formatted_results.append({'start': start, 'end': end, 'category': category, 'count': count})

    logger.info('✅ Binned data generated successfully!')
    return formatted_results


@router.get('/project_registration', response_model=list[ProjectBinnedRegistration])
def get_project_registration(
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

    return get_binned_data(session=session, num_bins=num_bins, projects=filtered_projects)
