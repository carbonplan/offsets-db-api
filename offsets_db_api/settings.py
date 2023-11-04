import pydantic


class Settings(pydantic.BaseSettings):
    database_url: str = pydantic.Field(..., env='offsets_db_database_url')
    staging: bool = pydantic.Field(default=True, env='offsets_db_staging')
    api_key: str = pydantic.Field(default=None, env='offsets_db_api_key')
    export_path: str = pydantic.Field(
        default='data/export',
        env='offsets_db_export_path',
        description='Path to export database tables to.',
    )

    @pydantic.validator('database_url', pre=True)
    def fix_database_url(cls, value: str) -> str:
        # fix unsupported URI scheme:
        # https://stackoverflow.com/questions/62688256/sqlalchemy-exc-nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectspostgre/67754795#67754795
        if value is not None and value.startswith('postgres://'):
            return value.replace('postgres://', 'postgresql://', 1)

        return value

    class Config:
        env_file = '.env'
        env_prefix = 'offsets_db_'


def get_settings() -> Settings:
    return Settings()


def generate_api_secret_key(length: int = 32) -> str:
    import secrets

    return secrets.token_urlsafe(length)
