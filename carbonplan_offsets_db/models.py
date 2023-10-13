import datetime

import pydantic
from sqlalchemy.dialects import postgresql
from sqlmodel import BigInteger, Column, Field, SQLModel, String

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


class Project(SQLModel, table=True):
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
    details_url: pydantic.HttpUrl | None = Field(description='URL to project details')
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.now, description='Date project was recorded in database'
    )


class Credit(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.now, description='Date credit was recorded in database'
    )
    project_id: str = Field(description='Project id used by registry system')
    quantity: int = Field(description='Number of credits', sa_column=Column(BigInteger()))
    vintage: int | None = Field(description='Vintage year of credits')
    transaction_date: datetime.date | None = Field(description='Date of transaction')
    transaction_type: str | None = Field(description='Type of transaction')


class ProjectWithPagination(pydantic.BaseModel):
    pagination: Pagination
    data: list[Project]


class CreditWithPagination(pydantic.BaseModel):
    pagination: Pagination
    data: list[Credit]


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


class ProjectBinnedRegistration(pydantic.BaseModel):
    start: datetime.date | None
    end: datetime.date | None
    category: str | None
    value: int | None


class ProjectBinnedCreditsTotals(pydantic.BaseModel):
    start: float | None
    end: float | None
    category: str | None
    value: float | None
