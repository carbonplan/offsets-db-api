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
    chunksize: int = 10_000
    valid_records_file_url: pydantic.AnyUrl | None = None


class FileURLResponse(pydantic.BaseModel):
    message: str
    file_url: pydantic.AnyUrl


class Pagination(pydantic.BaseModel):
    total_entries: int
    current_page: int
    total_pages: int
    next_page: pydantic.AnyHttpUrl | None
