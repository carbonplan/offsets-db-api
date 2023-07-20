import datetime

import pydantic
from sqlalchemy import BigInteger, Column
from sqlmodel import Field, Relationship, SQLModel

from .schemas import FileCategory, FileStatus, Pagination


class FileBase(SQLModel):
    url: pydantic.AnyUrl
    content_hash: str | None = Field(description='Hash of file contents')
    status: FileStatus = Field(default='pending', description='Status of file processing')
    error: str | None = Field(description='Error message if processing failed')
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow, description='Date file was recorded in database'
    )
    category: FileCategory = Field(description='Category of file', default='unknown')
    chunksize: int | None = Field(description='Chunksize used to process file', default=None)
    valid_records_file_url: pydantic.AnyUrl | None = Field(
        description='URL to valid records file. This is only used when removing stale records from the database.',
        default=None,
    )


class File(FileBase, table=True):
    id: int = Field(default=None, primary_key=True)


class ProjectBase(SQLModel):
    project_id: str = Field(description='Project id used by registry system', unique=True)
    name: str | None = Field(description='Name of the project')
    registry: str = Field(description='Name of the registry')
    proponent: str | None
    protocol: str | None
    category: str | None
    developer: str | None
    voluntary_status: str | None
    country: str | None
    started_at: datetime.date | None = Field(description='Date project started')
    registered_at: datetime.date | None = Field(description='Date project was registered')
    is_arb: bool | None = Field(description='Whether project is an ARB project')
    retired: int | None = Field(
        description='Total of retired credits', default=0, sa_column=Column(BigInteger())
    )
    issued: int | None = Field(
        description='Total of issued credits', default=0, sa_column=Column(BigInteger())
    )


class Project(ProjectBase, table=True):
    id: int = Field(default=None, primary_key=True)

    # relationship
    credits: list['Credit'] = Relationship(back_populates='project')
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.now, description='Date project was recorded in database'
    )
    description: str | None
    details_url: pydantic.HttpUrl | None = Field(description='URL to project details')


class ProjectRead(ProjectBase):
    id: int
    description: str | None
    details_url: pydantic.HttpUrl | None


class CreditBase(SQLModel):
    project_id: str = Field(
        description='Project id used by registry system', foreign_key='project.project_id'
    )
    quantity: int = Field(description='Number of credits', sa_column=Column(BigInteger()))
    vintage: int | None = Field(description='Vintage year of credits')
    transaction_date: datetime.date | None = Field(description='Date of transaction')
    transaction_type: str | None = Field(description='Type of transaction')
    transaction_serial_number: str = Field(description='Transaction serial number', unique=True)
    details_url: pydantic.HttpUrl | None = Field(description='URL to unit information report')


class Credit(CreditBase, table=True):
    id: int = Field(default=None, primary_key=True)
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.now, description='Date credit was recorded in database'
    )

    # relationship
    project: Project = Relationship(back_populates='credits')


class CreditRead(CreditBase):
    id: int


class ProjectReadDetails(ProjectRead):
    recorded_at: datetime.datetime
    credits: list['CreditRead']


class ProjectWithPagination(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectRead]


class CreditWithPagination(pydantic.BaseModel):
    pagination: Pagination
    data: list[CreditRead]


class ProjectStats(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    date: datetime.date
    registry: str
    total_projects: int


class CreditStats(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    date: datetime.date
    registry: str
    transaction_type: str
    total_credits: int = Field(sa_column=Column(BigInteger()))
    total_transactions: int | None


class CreditStatsWithPagination(pydantic.BaseModel):
    pagination: Pagination
    data: list[CreditStats]


class ProjectStatsWithPagination(pydantic.BaseModel):
    pagination: Pagination
    data: list[ProjectStats]
