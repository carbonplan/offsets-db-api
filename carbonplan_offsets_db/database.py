from sqlmodel import Session, create_engine

from .settings import get_settings


def get_session() -> Session:
    settings = get_settings()
    engine = create_engine(settings.database_url, connect_args={'options': '-c timezone=utc'})
    with Session(engine) as session:
        yield session
