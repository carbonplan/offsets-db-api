from collections.abc import Generator

from sqlmodel import Session, create_engine

from .settings import get_settings

# https://github.com/tiangolo/full-stack-fastapi-postgresql/issues/104#issuecomment-586466934
DB_POOL_SIZE = 400
WEB_CONCURRENCY = 8
POOL_SIZE = max(DB_POOL_SIZE // WEB_CONCURRENCY, 5)


def get_engine(*, database_url: str):
    return create_engine(
        database_url,
        connect_args={'options': '-c timezone=utc'},
        pool_size=POOL_SIZE,
        pool_pre_ping=True,
    )


def get_session() -> Generator[Session, None, None]:
    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)
    with Session(engine) as session:
        yield session
