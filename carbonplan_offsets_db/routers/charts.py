import datetime

from fastapi import APIRouter, Depends, Request
from sqlmodel import Session, and_, case, func

from ..database import get_session
from ..logging import get_logger
from ..models import Project

router = APIRouter()
logger = get_logger()


def get_binned_data(session, num_bins):
    # Get the min and max date from registered_at
    min_date, max_date = session.query(
        func.min(Project.registered_at), func.max(Project.registered_at)
    ).one()

    # Calculate the bin width
    bin_width = (max_date - min_date) / num_bins

    # Define the binning logic using a combination of the CASE statement and basic arithmetic
    conditions = [
        (
            and_(
                Project.registered_at >= min_date + i * bin_width,
                Project.registered_at < min_date + (i + 1) * bin_width,
            ),
            f'{(min_date + i*bin_width).year}-{(min_date + (i+1)*bin_width).year}',
        )
        for i in range(num_bins - 1)
    ]
    last_bin_label = (
        f'{(min_date + (num_bins-1)*bin_width).year}-present'
        if max_date.year == datetime.datetime.now().year
        else f'{(min_date + (num_bins-1)*bin_width).year}-{max_date.year}'
    )
    conditions.append(
        (Project.registered_at >= min_date + (num_bins - 1) * bin_width, last_bin_label)
    )

    binned_date = case(conditions, else_='other').label('bin')

    # Query with the binning logic
    binned_results = (
        session.query(binned_date, Project.category, func.count(Project.id).label('count'))
        .group_by(binned_date, Project.category)
        .all()
    )

    total_projects = session.query(Project).count()
    total_binned_counts = sum(result[2] for result in binned_results)

    if total_projects != total_binned_counts:
        raise ValueError(
            f'Total projects ({total_projects}) does not match sum of binned counts ({total_binned_counts}).'
        )

    return binned_results


@router.get('/project_registration')
def get_project_registration(request: Request, session: Session = Depends(get_session)):
    """Get project registration data"""
    logger.info(f'Getting project registration data: {request.url}')

    results = get_binned_data(session, 15)
    return results
