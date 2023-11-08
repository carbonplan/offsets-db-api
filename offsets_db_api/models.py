import datetime

import pandas as pd
import pandera as pa
import pydantic
from sqlalchemy.dialects import postgresql
from sqlmodel import BigInteger, Column, Field, Relationship, SQLModel, String

from .schemas import FileCategory, FileStatus, Pagination


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
        description='Project id used by registry system', primary_key=True, index=True, unique=True
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
    credits: list['Credit'] = Relationship(
        back_populates='project',
        sa_relationship_kwargs={
            'cascade': 'all,delete,delete-orphan',  # Instruct the ORM how to track changes to local objects
        },
    )
    clip_relationships: list['ClipProject'] = Relationship(
        back_populates='project', sa_relationship_kwargs={'cascade': 'all,delete,delete-orphan'}
    )


class ClipBase(SQLModel):
    date: datetime.datetime = Field(description='Date the clip was published')
    title: str | None = Field(description='Title of the clip')
    url: pydantic.AnyUrl | None = Field(description='URL to the clip')
    source: str | None = Field(description='Source of the clip')
    tags: list[str] | None = Field(
        description='Tags associated with the clip', sa_column=Column(postgresql.ARRAY(String()))
    )
    notes: str | None = Field(description='Notes associated with the clip')
    is_waybacked: bool | None = Field(
        default=False, description='Whether the clip is a waybacked clip'
    )
    type: str = Field(description='Type of clip', default='unknown')


class Clip(ClipBase, table=True):
    id: int = Field(default=None, primary_key=True)
    project_relationships: list['ClipProject'] = Relationship(
        back_populates='clip', sa_relationship_kwargs={'cascade': 'all,delete,delete-orphan'}
    )


class ClipwithProjects(ClipBase):
    id: int
    project_ids: list[str] = Field(
        default=[], description='List of project ids associated with clip'
    )


class ClipProject(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    clip_id: int = Field(description='Id of clip', foreign_key='clip.id')
    project_id: str = Field(description='Id of project', foreign_key='project.project_id')
    clip: Clip | None = Relationship(back_populates='project_relationships')
    project: Project | None = Relationship(back_populates='clip_relationships')


class ProjectWithClips(ProjectBase):
    clips: list[Clip] | None = Field(
        default=None, description='List of clips associated with project'
    )


class Credit(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    project_id: str | None = Field(
        description='Project id used by registry system',
        index=True,
        foreign_key='project.project_id',
    )
    quantity: int = Field(description='Number of credits', sa_column=Column(BigInteger()))
    vintage: int | None = Field(description='Vintage year of credits')
    transaction_date: datetime.date | None = Field(description='Date of transaction')
    transaction_type: str | None = Field(description='Type of transaction')
    project: Project | None = Relationship(
        back_populates='credits',
    )


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


class ProjectCreditTotals(pydantic.BaseModel):
    start: datetime.date | None
    end: datetime.date | None
    value: int | None


class PaginatedProjectCreditTotals(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectCreditTotals]


class ProjectCounts(pydantic.BaseModel):
    category: str
    value: int


class CreditCounts(pydantic.BaseModel):
    category: str
    retired: int
    issued: int


class PaginatedProjectCounts(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectCounts]


class PaginatedCreditCounts(pydantic.BaseModel):
    pagination: Pagination
    data: list[CreditCounts]


class ProjectBinnedCreditsTotals(pydantic.BaseModel):
    start: float | None
    end: float | None
    category: str | None
    value: float | None


class PaginatedBinnedCreditTotals(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectBinnedCreditsTotals]


class PaginatedClips(pydantic.BaseModel):
    pagination: Pagination
    data: list[ClipwithProjects]
