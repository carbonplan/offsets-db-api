import pytest
from fastapi.testclient import TestClient

from carbonplan_offsets_db.database import get_session
from carbonplan_offsets_db.main import create_application
from carbonplan_offsets_db.settings import Settings, get_settings


def get_settings_override():
    return Settings(staging=True, api_key='bar')


@pytest.fixture(scope='function')
def test_db_session():
    session = get_session()
    yield session
    session.close()


@pytest.fixture(scope='module')
def test_app():
    app = create_application()
    app.dependency_overrides[get_settings] = get_settings_override

    with TestClient(app) as test_client:
        yield test_client
