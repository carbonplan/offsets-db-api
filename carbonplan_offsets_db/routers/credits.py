from fastapi import APIRouter, Depends
from sqlmodel import Session

from ..database import get_session
from ..logging import get_logger
from ..models import Credit, CreditRead

router = APIRouter()
logger = get_logger()


@router.get('/', summary='List credits', response_model=list[CreditRead])
def get_credits(session: Session = Depends(get_session)):
    """List credits"""
    logger.info('Getting credits')
    results = session.query(Credit).all()
    logger.info(f'Found {len(results)} credits')
    return results
