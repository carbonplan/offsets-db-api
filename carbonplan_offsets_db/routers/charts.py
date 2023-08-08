from fastapi import APIRouter, Depends, Request
from sqlmodel import Session, text

from ..database import get_session
from ..logging import get_logger

router = APIRouter()
logger = get_logger()


@router.get('/project_registration')
def get_project_registration(request: Request, session: Session = Depends(get_session)):
    """Get project registration data"""
    logger.info(f'Getting project registration data: {request.url}')
    stmt = text(
        """
        SELECT
            width_bucket(
                extract(year FROM age(now(), registered_at)),
                0,
                2,
                15
            ) AS bin,
            count(*)
        FROM
            project
        GROUP BY
            bin
        ORDER BY
            bin
    """
    )

    result = session.execute(stmt)
    return result.all()
