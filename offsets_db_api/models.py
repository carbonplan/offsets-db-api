import datetime

import pydantic
from sqlalchemy.dialects import postgresql
from sqlmodel import BigInteger, Column, Field, Relationship, SQLModel, String

from .schemas import FileCategory, FileStatus, Pagination


class File(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    url: str
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
    listed_at: datetime.datetime | None = Field(description='Date project was listed')
    is_compliance: bool | None = Field(description='Whether project is compliance project')
    retired: int | None = Field(
        description='Total of retired credits', default=0, sa_column=Column(BigInteger())
    )
    issued: int | None = Field(
        description='Total of issued credits', default=0, sa_column=Column(BigInteger())
    )
    first_issuance_at: datetime.datetime | None = Field(
        description='Date of first issuance of credits'
    )
    first_retirement_at: datetime.datetime | None = Field(
        description='Date of first retirement of credits'
    )
    project_url: str | None = Field(description='URL to project details')


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
    url: str | None = Field(description='URL to the clip')
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


class ProjectInfo(pydantic.BaseModel):
    project_id: str
    category: list[str] | None = Field(description='List of categories', default=None)


class ClipwithProjects(ClipBase):
    id: int
    projects: list[ProjectInfo]


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


class CreditBase(SQLModel):
    quantity: int = Field(description='Number of credits', sa_column=Column(BigInteger()))
    vintage: int | None = Field(description='Vintage year of credits')
    transaction_date: datetime.datetime | None = Field(description='Date of transaction')
    transaction_type: str | None = Field(description='Type of transaction')


class Credit(CreditBase, table=True):
    id: int = Field(default=None, primary_key=True)
    project_id: str | None = Field(
        description='Project id used by registry system',
        index=True,
        foreign_key='project.project_id',
    )
    project: Project | None = Relationship(
        back_populates='credits',
    )


class CreditWithCategory(CreditBase):
    id: int
    projects: list[ProjectInfo]


class PaginatedProjects(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectWithClips]


class PaginatedCredits(pydantic.BaseModel):
    pagination: Pagination
    data: list[CreditWithCategory]


class BinnedValues(pydantic.BaseModel):
    start: datetime.datetime | None
    end: datetime.datetime | None
    category: str | None
    value: int | None


class PaginatedBinnedValues(pydantic.BaseModel):
    pagination: Pagination
    data: list[BinnedValues]


class ProjectCreditTotals(pydantic.BaseModel):
    start: datetime.datetime | None
    end: datetime.datetime | None
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
