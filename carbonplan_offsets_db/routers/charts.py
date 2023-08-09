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


def get_binned_data(*, session, num_bins, binning_attribute, projects=None):
    """
    This function bins the projects based on a specified attribute and groups them by category.

    Parameters
    ----------
    session: Session
        SQLAlchemy session for querying the database.
    num_bins: int
        Number of bins to divide the data into.
    binning_attribute: str
        Attribute name of the Project model to be used for binning (e.g., 'registered_at' or 'issued').
    projects: list, optional
        List of projects to be binned. If not provided, the function will query the entire Project table.

    Returns
    -------
    binned_results: list
        A list of dictionaries, each containing the bin start, end, category, and count of projects.
    """

    logger.info(f'📊 Generating binned data based on {binning_attribute}...')

    # Dynamically get the attribute from the Project model based on the provided binning_attribute
    attribute = getattr(Project, binning_attribute)

    # If projects are provided, extract values for the given binning_attribute. Otherwise, query the database.
    if projects:
        values = [
            getattr(project, binning_attribute)
            for project in projects
            if getattr(project, binning_attribute) is not None
        ]
        if not values:
            logger.error(f'❌ No valid values found for attribute {binning_attribute}!')
            raise ValueError(f'Provided projects have no valid values for {binning_attribute}.')
        min_value, max_value = min(values), max(values)
    else:
        # Get the minimum and maximum values for the attribute from the database
        min_value, max_value = session.query(func.min(attribute), func.max(attribute)).one()

    # Calculate the width for each bin
    bin_width = (max_value - min_value) / num_bins

    # Create conditions for each bin. These conditions will determine which bin a project falls into.
    # Check if the binning attribute is a date type
    if isinstance(min_value, datetime.date | datetime.datetime):
        # Create conditions for each bin. These conditions will determine which bin a project falls into for date attributes.
        conditions = [
            (
                and_(
                    attribute >= min_value + datetime.timedelta(days=i * bin_width.days),
                    attribute < min_value + datetime.timedelta(days=(i + 1) * bin_width.days),
                ),
                f'{(min_value + datetime.timedelta(days=i * bin_width.days)).year}-{(min_value + datetime.timedelta(days=(i + 1) * bin_width.days)).year}',
            )
            for i in range(num_bins)
        ]
    else:
        # Create conditions for each bin. These conditions will determine which bin a project falls into for numerical attributes.
        conditions = [
            (
                and_(
                    attribute >= min_value + i * bin_width,
                    attribute < min_value + (i + 1) * bin_width,
                ),
                f'{min_value + i*bin_width}-{min_value + (i+1)*bin_width}',
            )
            for i in range(num_bins)
        ]

    # Using the conditions, generate a CASE statement to assign a bin label to each project.
    binned_attribute = case(conditions, else_='other').label('bin')

    # Query the database, grouping by the calculated bin and category. Count the number of projects in each group.
    binned_results = (
        session.query(binned_attribute, Project.category, func.count(Project.id).label('count'))
        .group_by('bin', Project.category)
        .all()
    )

    # Validate that the counts from binned results match the total number of projects.
    total_projects = session.query(Project).count()
    total_binned_counts = sum(result[2] for result in binned_results)
    if total_projects != total_binned_counts:
        logger.error('❌ Mismatch in total counts!')
        raise ValueError(
            f"Total projects ({total_projects}) doesn't match sum of binned counts ({total_binned_counts})."
        )

    # Reformat results to be more user-friendly
    formatted_results = []
    for bin_label, category, count in binned_results:
        logger.info(bin_label)
        start, end = (part for part in bin_label.split('-')) if '-' in bin_label else (None, None)
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

    return get_binned_data(
        session=session,
        num_bins=num_bins,
        binning_attribute='registered_at',
        projects=filtered_projects,
    )
