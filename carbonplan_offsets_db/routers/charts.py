from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, and_, case, func

from ..database import get_session
from ..logging import get_logger
from ..models import Project, ProjectBinnedRegistration

router = APIRouter()
logger = get_logger()


def get_binned_data(*, session, num_bins):
    """
    This function bins the projects based on their registration date and groups them by category.

    Parameters
    ----------
    session: Session
        SQLAlchemy session for querying the database.
    num_bins: int,
        Number of bins to divide the registration dates into.

    Returns
    -------
    binned_results: list
        A list of tuples, each containing the bin label, category, and count of projects.
    """

    logger.info('📊 Generating binned data...')
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
    session: Session = Depends(get_session),
):
    """Get aggregated project registration data"""
    logger.info(f'Getting project registration data: {request.url}')

    return get_binned_data(session=session, num_bins=num_bins)
