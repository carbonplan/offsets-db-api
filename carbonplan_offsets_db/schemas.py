from __future__ import annotations

import typing

import pydantic

Registries = typing.Literal[
    'verra',
    'gold-standard',
    'global-carbon-council',
    'american-carbon-registry',
    'climate-action-reserve',
    'art-trees',
]


class FileURLPayload(pydantic.BaseModel):
    url: pydantic.AnyUrl


class FileURLResponse(pydantic.BaseModel):
    message: str
    file_url: pydantic.AnyUrl
