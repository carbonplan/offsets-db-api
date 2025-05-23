import datetime
import typing

import pydantic
from sqlalchemy.dialects import postgresql
from sqlmodel import BigInteger, Column, Enum, Field, Index, Relationship, SQLModel, String, text

from offsets_db_api.schemas import FileCategory, FileStatus, Pagination


class File(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True, index=True)
    url: str
    content_hash: str | None = Field(description='Hash of file contents')
    status: FileStatus = Field(default='pending', description='Status of file processing')
    error: str | None = Field(description='Error message if processing failed')
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow, description='Date file was recorded in database'
    )
    category: FileCategory = Field(
        description='Category of file', sa_column=Column(Enum(FileCategory))
    )


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
    category: str | None = Field(description='Category of the project')
    status: str | None
    country: str | None
    listed_at: datetime.date | None = Field(
        description='Date project was listed',
    )
    is_compliance: bool | None = Field(description='Whether project is compliance project')
    retired: int | None = Field(
        description='Total of retired credits', default=0, sa_column=Column(BigInteger())
    )
    issued: int | None = Field(
        description='Total of issued credits', default=0, sa_column=Column(BigInteger())
    )
    first_issuance_at: datetime.date | None = Field(
        description='Date of first issuance of credits',
    )
    first_retirement_at: datetime.date | None = Field(
        description='Date of first retirement of credits',
    )
    project_url: str | None = Field(description='URL to project details')
    project_type: str | None = Field(description='Type of project (e.g. ARB, VCS, etc.)')
    project_type_source: str | None = Field(description='Source of project type information')


class Project(ProjectBase, table=True):
    __table_args__ = (
        Index(
            'ix_project_project_id_gin',
            text('lower(project_id) gin_trgm_ops'),
            postgresql_using='gin',
        ),
        Index('ix_project_name_gin', text('lower(name) gin_trgm_ops'), postgresql_using='gin'),
    )
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
    date: datetime.date = Field(description='Date the clip was published')
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
    type: str | None = Field(description='Type of clip', default='unknown')


class Clip(ClipBase, table=True):
    id: int = Field(default=None, primary_key=True, index=True)
    project_relationships: list['ClipProject'] = Relationship(
        back_populates='clip', sa_relationship_kwargs={'cascade': 'all,delete,delete-orphan'}
    )


class ProjectInfo(pydantic.BaseModel):
    project_id: str
    category: str | None = Field(description='category')
    project_type: str | None = Field(description='Type of project ')


class ClipwithProjects(ClipBase):
    id: int
    projects: list[ProjectInfo]


class ClipProject(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True, index=True)
    clip_id: int = Field(description='Id of clip', foreign_key='clip.id', index=True)
    project_id: str = Field(
        description='Id of project', foreign_key='project.project_id', index=True
    )
    clip: Clip | None = Relationship(back_populates='project_relationships')
    project: Project | None = Relationship(back_populates='clip_relationships')


class ProjectWithClips(ProjectBase):
    clips: list[Clip] | None = Field(
        default=None, description='List of clips associated with project'
    )


class CreditBase(SQLModel):
    quantity: int = Field(description='Number of credits', sa_column=Column(BigInteger()))
    vintage: int | None = Field(description='Vintage year of credits')
    transaction_date: datetime.date | None = Field(description='Date of transaction', index=True)
    transaction_type: str | None = Field(description='Type of transaction')
    retirement_account: str | None = Field(description='Account used for the transaction')
    retirement_beneficiary: str | None = Field(description='Beneficiary of credits')
    retirement_reason: str | None = Field(description='Reason for transaction')
    retirement_note: str | None = Field(description='Note')
    retirement_beneficiary_harmonized: str | None = Field(
        description='Harmonized beneficiary of credits'
    )


class Credit(CreditBase, table=True):
    __table_args__ = (
        Index(
            'ix_credit_transaction_type_gin',
            text('lower(transaction_type) gin_trgm_ops'),
            postgresql_using='gin',
        ),
        Index(
            'ix_credit_retirement_account_gin',
            text('lower(retirement_account) gin_trgm_ops'),
            postgresql_using='gin',
        ),
        Index(
            'ix_credit_retirement_beneficiary_gin',
            text('lower(retirement_beneficiary) gin_trgm_ops'),
            postgresql_using='gin',
        ),
        Index(
            'ix_credit_retirement_reason_gin',
            text('lower(retirement_reason) gin_trgm_ops'),
            postgresql_using='gin',
        ),
        Index(
            'ix_credit_retirement_note_gin',
            text('lower(retirement_note) gin_trgm_ops'),
            postgresql_using='gin',
        ),
        Index(
            'ix_credit_retirement_beneficiary_harmonized_gin',
            text('lower(retirement_beneficiary_harmonized) gin_trgm_ops'),
            postgresql_using='gin',
        ),
    )

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
    data: list[ProjectWithClips] | list[dict[str, typing.Any]]


class PaginatedCredits(pydantic.BaseModel):
    pagination: Pagination
    data: list[CreditWithCategory] | list[dict[str, typing.Any]]


class BinnedValues(pydantic.BaseModel):
    start: datetime.date | None = pydantic.Field(description='Start date of bin', strict=False)
    end: datetime.date | None = pydantic.Field(description='End date of bin', strict=False)
    category: str | None
    value: int | None


class PaginatedBinnedValues(pydantic.BaseModel):
    pagination: Pagination
    data: list[BinnedValues] | list[dict[str, typing.Any]]


class ProjectCreditTotals(pydantic.BaseModel):
    start: datetime.date | None = pydantic.Field(description='Start date of bin', strict=False)
    end: datetime.date | None = pydantic.Field(description='End date of bin', strict=False)
    value: int | None


class PaginatedProjectCreditTotals(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectCreditTotals] | list[dict[str, typing.Any]]


class ProjectCounts(pydantic.BaseModel):
    category: str
    value: int


class CreditCounts(pydantic.BaseModel):
    category: str
    retired: int
    issued: int


class PaginatedProjectCounts(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectCounts] | list[dict[str, typing.Any]]


class PaginatedCreditCounts(pydantic.BaseModel):
    pagination: Pagination
    data: list[CreditCounts] | list[dict[str, typing.Any]]


class ProjectBinnedCreditsTotals(pydantic.BaseModel):
    start: float | None
    end: float | None
    category: str | None
    value: int | None


class PaginatedBinnedCreditTotals(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectBinnedCreditsTotals] | list[dict[str, typing.Any]]


class PaginatedClips(pydantic.BaseModel):
    pagination: Pagination
    data: list[ClipwithProjects] | list[dict[str, typing.Any]]


class PaginatedFiles(pydantic.BaseModel):
    pagination: Pagination
    data: list[File] | list[dict[str, typing.Any]]
