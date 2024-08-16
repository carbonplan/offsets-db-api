import pathlib
import typing

from fastapi import Request, Response
from fastapi_cache import FastAPICache

from offsets_db_api.log import get_logger
from offsets_db_api.query_helpers import _convert_query_params_to_dict

logger = get_logger()

CACHE_NAMESPACE = 'offsets-db'

app_dir = pathlib.Path(__file__).parent.parent

watch_dog_dir = app_dir / 'cache-watch-dog'
watch_dog_dir.mkdir(parents=True, exist_ok=True)
watch_dog_file = watch_dog_dir / 'last-db-update.txt'


def request_key_builder(
    func: typing.Callable[..., typing.Any],
    namespace: str = CACHE_NAMESPACE,
    *,
    request: Request,
    response: Response,
    **kwargs: typing.Any,
):
    params = _convert_query_params_to_dict(request)
    sorted_params = {
        key: sorted(params[key]) if isinstance(params[key], list) else params[key]
        for key in sorted(params)
    }
    return ':'.join(
        [
            namespace,
            request.method.lower(),
            request.url.path,
            repr(sorted_params),
        ]
    )


async def clear_cache():
    try:
        # List existing keys in cache
        keys = list(FastAPICache._backend._store.keys())

        if keys:
            formatted_keys = '\n'.join(f'🔑 {key}' for key in keys)
            logger.info(f'🔍 Found {len(keys)} keys to clear:\n{formatted_keys}')
        else:
            logger.info('🚫 No keys found in cache to clear.')

        # Clear cache
        logger.info('🧹 Clearing cache...')
        await FastAPICache.clear(namespace=CACHE_NAMESPACE)
        logger.info('✅ Cache successfully cleared!')
    except Exception as exc:
        logger.warning(f'❌ Failed to clear cache: {exc}', exc_info=True)
