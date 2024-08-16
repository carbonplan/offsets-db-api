from collections.abc import Generator

from sqlmodel import Session, create_engine

from offsets_db_api.settings import get_settings


def get_engine(*, database_url: str):
    settings = get_settings()
    # https://github.com/tiangolo/full-stack-fastapi-postgresql/issues/104#issuecomment-586466934
    pool_size = max(settings.database_pool_size // settings.web_concurrency, 5)
    return create_engine(
        database_url,
        connect_args={'options': '-c timezone=utc'},
        pool_size=pool_size,
        pool_pre_ping=True,
    )


def get_session() -> Generator[Session, None, None]:
    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)
    with Session(engine) as session:
        yield session
