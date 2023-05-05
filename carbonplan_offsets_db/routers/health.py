from fastapi import APIRouter, Depends

from ..security import check_api_key
from ..settings import Settings, get_settings

router = APIRouter()


@router.get('/')
def status(settings: Settings = Depends(get_settings)):
    return {'status': 'ok', 'staging': settings.staging}


@router.get('/authorized_user')
def validate_authorized_user(authorized_user: bool = Depends(check_api_key)):
    return {'authorized_user': authorized_user}
