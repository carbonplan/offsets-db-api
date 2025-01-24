from __future__ import annotations

import enum
import typing

import pydantic

Registries = typing.Literal[
    'verra',
    'gold-standard',
    'american-carbon-registry',
    'climate-action-reserve',
    'art-trees',
    'none',
]


class FileStatus(str, enum.Enum):
    pending = 'pending'
    success = 'success'
    failure = 'failure'


class FileCategory(str, enum.Enum):
    projects = 'projects'
    credits = 'credits'
    clips = 'clips'
    projecttypes = 'projecttypes'
    unknown = 'unknown'


class FileURLPayload(pydantic.BaseModel):
    url: str
    category: FileCategory


class FileURLResponse(pydantic.BaseModel):
    message: str
    file_url: str


class Pagination(pydantic.BaseModel):
    total_entries: int
    current_page: int
    total_pages: int
    next_page: str | None = None


class ProjectTypes(pydantic.BaseModel):
    top: list[str]
    others: list[str]
