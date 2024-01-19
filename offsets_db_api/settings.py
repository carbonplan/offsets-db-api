import pydantic
import pydantic_settings


class Settings(pydantic_settings.BaseSettings):
    model_config = pydantic_settings.SettingsConfigDict(env_prefix='offsets_db_')

    database_url: str = pydantic.Field(default=None)
    database_pool_size: int = pydantic.Field(default=300)
    web_concurrency: int = pydantic.Field(default=1)
    staging: bool = pydantic.Field(default=True)
    api_key: pydantic.SecretStr | None = pydantic.Field(default=None)

    @pydantic.validator('database_url', pre=True)
    def fix_database_url(cls, value: str) -> str:
        # fix unsupported URI scheme:
        # https://stackoverflow.com/questions/62688256/sqlalchemy-exc-nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectspostgre/67754795#67754795
        if value is not None and value.startswith('postgres://'):
            return value.replace('postgres://', 'postgresql://', 1)

        return value


def get_settings() -> Settings:
    return Settings()


def generate_api_secret_key(length: int = 32) -> str:
    import secrets

    return secrets.token_urlsafe(length)
