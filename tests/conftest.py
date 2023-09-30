import time

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


@pytest.fixture(scope='session')
def test_app():
    app = create_application()
    app.dependency_overrides[get_settings] = get_settings_override

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(scope='session', autouse=True)
def setup_post(test_app):
    payload = (
        [
            {
                'url': 's3://carbonplan-share/offsets-db-testing-data/data/processed/latest/verra/transactions.parquet',
                'category': 'projects',
            },
            {
                'url': 's3://carbonplan-share/offsets-db-testing-data/data/processed/latest/verra/transactions.parquet',
                'category': 'credits',
            },
        ],
    )
    test_app.post('/files', json=payload)

    timeout = time.time() + 10  # 10 seconds from now
    while True:
        if time.time() > timeout:
            break  # or raise an exception

        # TODO: Implement check here. If it passes, then break.
        # if condition():
        #     break

        time.sleep(1)  # Sleep for 1 second
