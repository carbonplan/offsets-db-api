import hmac

import pydantic
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from offsets_db_api.settings import Settings, get_settings

api_key_header = APIKeyHeader(name='X-API-KEY', auto_error=False)


def api_key_extractor(settings: Settings = Depends(get_settings)):
    return settings.api_key


def check_api_key(
    api_key_header: str = Security(api_key_header),
    api_key: pydantic.SecretStr = Depends(api_key_extractor),
):
    """Check that the API key in the header matches the API key in the settings."""

    if api_key_header is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Missing API key. Please provide one in the X-API-KEY header.',
        )

    if hmac.compare_digest(api_key_header, api_key.get_secret_value()):
        return True
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Bad API key credentials. Please check your API key in the X-API-KEY header and try again.',
        )
