import datetime
import enum

import pydantic
from sqlmodel import Field, SQLModel


class FileStatus(str, enum.Enum):
    pending = 'pending'
    success = 'success'
    failure = 'failure'


class FileCategory(str, enum.Enum):
    projects = 'projects'
    credits = 'credits'
    unknown = 'unknown'


class FileBase(SQLModel):
    url: pydantic.AnyUrl
    content_hash: str | None = Field(description='Hash of file contents')
    status: FileStatus = Field(default='pending', description='Status of file processing')
    error: str | None = Field(description='Error message if processing failed')
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow, description='Date file was recorded in database'
    )
    category: FileCategory = Field(description='Category of file', default='unknown')


class File(FileBase, table=True):
    id: int = Field(default=None, primary_key=True)


class ProjectBase(SQLModel):
    project_id: str = Field(description='Project id used by registry system')
    name: str | None = Field(description='Name of the project')
    registry: str = Field(description='Name of the registry')
    proponent: str | None
    protocol: str | None
    developer: str | None
    voluntary_status: str | None
    country: str | None
    started_at: datetime.date | None = Field(description='Date project started')
    registered_at: datetime.date | None = Field(description='Date project was registered')


class Project(ProjectBase, table=True):
    id: int = Field(default=None, primary_key=True)
    recorded_at: datetime.datetime = Field(
        default_factory=datetime.datetime.now, description='Date project was recorded in database'
    )
    description: str | None
    details_url: pydantic.HttpUrl | None = Field(description='URL to project details')


class ProjectRead(ProjectBase):
    id: int
    description: str | None
    details_url: pydantic.HttpUrl | None


class ProjectReadDetails(ProjectRead):
    recorded_at: datetime.datetime
