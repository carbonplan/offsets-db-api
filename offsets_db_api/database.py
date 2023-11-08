from sqlmodel import Session, create_engine

from .settings import get_settings


def get_engine(*, database_url: str):
    return create_engine(database_url, connect_args={'options': '-c timezone=utc'})


def get_session() -> Session:
    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)
    with Session(engine) as session:
        yield session
