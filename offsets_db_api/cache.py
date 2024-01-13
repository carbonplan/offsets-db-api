import typing

from fastapi import Request, Response

from .query_helpers import _convert_query_params_to_dict

CACHE_NAMESPACE = 'offsets-db'


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
