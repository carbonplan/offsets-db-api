import datetime

import pandas as pd
import pandera as pa
import pydantic
from sqlalchemy.dialects import postgresql
from sqlmodel import BigInteger, Column, Field, SQLModel, String

from .schemas import FileCategory, FileStatus, Pagination


class Clip(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    project_id: str = Field(description='Project id used by registry system')
    published_at: datetime.datetime = Field(description='Date the clip was published')
    title: str | None = Field(description='Title of the clip')
    url: pydantic.AnyUrl | None = Field(description='URL to the clip')
    tags: list[str] | None = Field(
        description='Tags associated with the clip', sa_column=Column(postgresql.ARRAY(String()))
    )
    notes: str | None = Field(description='Notes associated with the clip')
    is_waybacked: bool = Field(default=False, description='Whether the clip is a waybacked clip')
    article_type: str = Field(description='Type of clip', default='unknown')


class File(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    url: pydantic.AnyUrl
    content_hash: str | None = Field(description='Hash of file contents')
    status: FileStatus = Field(default='pending', description='Status of file processing')
    error: str | None = Field(description='Error message if processing failed')
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow, description='Date file was recorded in database'
    )
    category: FileCategory = Field(description='Category of file', default='unknown')


class ProjectBase(SQLModel):
    project_id: str = Field(
        description='Project id used by registry system', unique=True, primary_key=True
    )
    name: str | None = Field(description='Name of the project')
    registry: str = Field(description='Name of the registry')
    proponent: str | None
    protocol: list[str] | None = Field(
        description='List of protocols', default=None, sa_column=Column(postgresql.ARRAY(String()))
    )
    category: list[str] | None = Field(
        description='List of categories', default=None, sa_column=Column(postgresql.ARRAY(String()))
    )
    status: str | None
    country: str | None
    listed_at: datetime.date | None = Field(description='Date project was listed')
    is_compliance: bool | None = Field(description='Whether project is compliance project')
    retired: int | None = Field(
        description='Total of retired credits', default=0, sa_column=Column(BigInteger())
    )
    issued: int | None = Field(
        description='Total of issued credits', default=0, sa_column=Column(BigInteger())
    )
    project_url: pydantic.HttpUrl | None = Field(description='URL to project details')


class Project(ProjectBase, table=True):
    ...


class ProjectWithClips(ProjectBase):
    clips: list[Clip] = Field(default=[], description='List of clips associated with project')


class Credit(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    project_id: str = Field(description='Project id used by registry system')
    quantity: int = Field(description='Number of credits', sa_column=Column(BigInteger()))
    vintage: int | None = Field(description='Vintage year of credits')
    transaction_date: datetime.date | None = Field(description='Date of transaction')
    transaction_type: str | None = Field(description='Type of transaction')


# Schema for 'project' table
project_schema = pa.DataFrameSchema(
    {
        'protocol': pa.Column(pa.Object, nullable=True),  # Array of strings
        'category': pa.Column(pa.Object, nullable=True),  # Array of strings
        'retired': pa.Column(
            pa.Int, pa.Check.greater_than_or_equal_to(0), nullable=True, coerce=True
        ),
        'issued': pa.Column(
            pa.Int, pa.Check.greater_than_or_equal_to(0), nullable=True, coerce=True
        ),
        'project_id': pa.Column(pa.String, nullable=False),
        'name': pa.Column(pa.String, nullable=True),
        'registry': pa.Column(pa.String, nullable=False),
        'proponent': pa.Column(pa.String, nullable=True),
        'status': pa.Column(pa.String, nullable=True),
        'country': pa.Column(pa.String, nullable=True),
        'listed_at': pa.Column(pd.DatetimeTZDtype(tz='UTC'), nullable=True, required=False),
        'is_compliance': pa.Column(pa.Bool, nullable=True),
        'project_url': pa.Column(pa.String, nullable=True),
    }
)

# Schema for 'credit' table
credit_schema = pa.DataFrameSchema(
    {
        'quantity': pa.Column(
            pa.Int, pa.Check.greater_than_or_equal_to(0), nullable=True, coerce=True
        ),
        'id': pa.Column(pa.Int, nullable=False),
        'project_id': pa.Column(pa.String, nullable=False),
        'vintage': pa.Column(pa.Int, nullable=True, coerce=True),
        'transaction_date': pa.Column(pd.DatetimeTZDtype(tz='UTC'), nullable=True),
        'transaction_type': pa.Column(pa.String, nullable=True),
    }
)

# Schema for 'clip' table
clip_schema = pa.DataFrameSchema(
    {
        'id': pa.Column(pa.Int, nullable=False),
        'project_id': pa.Column(pa.String, nullable=False),
        'published_at': pa.Column(pd.DatetimeTZDtype(tz='UTC'), nullable=True),
        'title': pa.Column(pa.String, nullable=True),
        'url': pa.Column(pa.String, nullable=True),
        'tags': pa.Column(pa.Object, nullable=True),
        'notes': pa.Column(pa.String, nullable=True),
        'is_waybacked': pa.Column(pa.Bool, nullable=True),
        'article_type': pa.Column(pa.String, nullable=True),
    }
)


class PaginatedProjects(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectWithClips]


class PaginatedCredits(pydantic.BaseModel):
    pagination: Pagination
    data: list[Credit]


class BinnedValues(pydantic.BaseModel):
    start: datetime.date | None
    end: datetime.date | None
    category: str | None
    value: int | None


class PaginatedBinnedValues(pydantic.BaseModel):
    pagination: Pagination
    data: list[BinnedValues]


class ProjectBinnedCreditsTotals(pydantic.BaseModel):
    start: float | None
    end: float | None
    category: str | None
    value: float | None


class PaginatedBinnedCreditTotals(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectBinnedCreditsTotals]


class ClipWithPagination(pydantic.BaseModel):
    pagination: Pagination
    data: list[Clip]
