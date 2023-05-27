from __future__ import annotations

import enum
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


class FileStatus(str, enum.Enum):
    pending = 'pending'
    success = 'success'
    failure = 'failure'


class FileCategory(str, enum.Enum):
    projects = 'projects'
    credits = 'credits'
    unknown = 'unknown'


class FileURLPayload(pydantic.BaseModel):
    url: pydantic.AnyUrl
    category: FileCategory


class FileURLResponse(pydantic.BaseModel):
    message: str
    file_url: pydantic.AnyUrl
