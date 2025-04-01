from __future__ import annotations

import datetime
import enum
import typing

import pydantic
from fastapi import Query

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
    Top: list[str | None]
    Other: list[str | None]


class ProjectFilters(pydantic.BaseModel):
    registry: list[Registries] | None = None
    country: list[str] | None = None
    protocol: list[str] | None = None
    category: list[str] | None = None
    type: list[str] | None = None
    is_compliance: bool | None = None
    listed_at_from: datetime.datetime | datetime.date | None = None
    listed_at_to: datetime.datetime | datetime.date | None = None
    issued_min: int | None = None
    issued_max: int | None = None
    retired_min: int | None = None
    retired_max: int | None = None


def get_project_filters(
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    listed_at_from: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    listed_at_to: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
):
    """Dependency to get project filters from query parameters"""
    return ProjectFilters(
        registry=registry,
        country=country,
        protocol=protocol,
        category=category,
        is_compliance=is_compliance,
        listed_at_from=listed_at_from,
        listed_at_to=listed_at_to,
        issued_min=issued_min,
        issued_max=issued_max,
        retired_min=retired_min,
        retired_max=retired_max,
    )


class BeneficiaryFilters(pydantic.BaseModel):
    beneficiary_search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on specified beneficiary_search_fields only.',
    )
    beneficiary_search_fields: list[str] = Query(
        default=['retirement_beneficiary_harmonized'],
        description='Beneficiary fields to search in',
    )


def get_beneficiary_filters(
    beneficiary_search: str | None = Query(
        None,
        description='Case insensitive search string. Currently searches on specified beneficiary_search_fields only.',
    ),
    beneficiary_search_fields: list[str] = Query(
        default=['retirement_beneficiary_harmonized'],
        description='Beneficiary fields to search in. Valid fields are: `retirement_beneficiary_harmonized`, `retirement_beneficiary`, `retirement_beneficiary_account`, `retirement_beneficiary_note`, `retirement_beneficiary_reason`',
    ),
):
    """Dependency to get beneficiary search params from query parameters"""
    return BeneficiaryFilters(
        beneficiary_search=beneficiary_search,
        beneficiary_search_fields=beneficiary_search_fields,
    )


class CreditFilters(pydantic.BaseModel):
    transaction_type: list[str] | None = None
    vintage: list[int] | None = None
    transaction_date_from: datetime.datetime | datetime.date | None = None
    transaction_date_to: datetime.datetime | datetime.date | None = None


def get_credit_filters(
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    vintage: list[int] | None = Query(None, description='Vintage'),
    transaction_date_from: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    transaction_date_to: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
):
    """Dependency to get credit filters from query parameters"""
    return CreditFilters(
        transaction_type=transaction_type,
        vintage=vintage,
        transaction_date_from=transaction_date_from,
        transaction_date_to=transaction_date_to,
    )
