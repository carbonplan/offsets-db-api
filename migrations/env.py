# flake8: noqa

import os
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

# Load .env before settings are read so OFFSETS_DB_DATABASE_URL is available
# whether running via `pixi run migrate` or a bare `alembic` invocation.
load_dotenv(override=False)

from offsets_db_api.models import (  # (be sure to import all models you need migrated)
    Clip,
    ClipProject,
    Credit,
    File,
    Project,
)
from offsets_db_api.settings import get_settings

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# https://stackoverflow.com/questions/37890284/ini-file-load-environment-variable
settings = get_settings()
database_url = settings.database_url
if database_url is None:
    raise RuntimeError(
        'OFFSETS_DB_DATABASE_URL is not set. '
        'Copy .env.example to .env and set the database URL, '
        'or export the variable in your shell before running migrations.'
    )
if database_url.startswith('postgres://'):
    # Fix Heroku's incompatible postgres database uri
    # https://stackoverflow.com/a/67754795/3266235
    database_url = database_url.replace('postgres://', 'postgresql://', 1)

config.set_main_option('sqlalchemy.url', database_url)
# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = SQLModel.metadata


def include_object(object, name, type_, reflected, compare_to):
    """
    Filter out objects that Alembic should not manage.

    Specifically, skip GIN/functional indexes that come back from
    PostgreSQL reflection with mangled names or expressions — Alembic
    cannot round-trip those accurately and would otherwise emit
    spurious drop/create pairs on every autogenerate run.
    """
    if type_ == 'index' and reflected and compare_to is None:
        # Index exists in DB but not in metadata — skip if it looks like
        # one of our functional (GIN) indexes so we don't drop it.
        if name and name.endswith('_gin'):
            return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option('sqlalchemy.url')
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
        compare_type=True,
        compare_server_default=True,
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
