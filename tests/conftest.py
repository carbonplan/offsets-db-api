import time

import pytest
from fastapi.testclient import TestClient

from carbonplan_offsets_db.database import get_session
from carbonplan_offsets_db.main import create_application
from carbonplan_offsets_db.settings import Settings, get_settings


# Override settings for tests
def get_settings_override():
    return Settings(staging=True, api_key='bar')


# Fixture to provide a database session per test function
@pytest.fixture(scope='function')
def test_db_session():
    session = get_session()
    yield session  # Yield control to the test function
    session.close()  # Cleanup after test function is done


# Fixture to provide a test client for FastAPI app; reused across the test session
@pytest.fixture(scope='session')
def test_app():
    app = create_application()
    app.dependency_overrides[get_settings] = get_settings_override

    with TestClient(app) as test_client:
        yield test_client  # Yield control to dependent fixtures and tests


# Auto-used fixture to perform a POST request once per test session
@pytest.fixture(scope='session', autouse=True)
def setup_post(test_app):
    # Define payload for POST request
    payload = [
        {
            'url': 's3://carbonplan-share/offsets-db-testing-data/final/projects-augmented.parquet',
            'category': 'projects',
        },
        {
            'url': 's3://carbonplan-share/offsets-db-testing-data/final/credits-augmented.parquet',
            'category': 'credits',
        },
    ]

    # Perform POST request
    post_response = test_app.post('/files', json=payload)
    print(f'POST response: {post_response.json()}')

    # Wait until files are processed or 10-second timeout is reached
    timeout = time.time() + 20  # 10 seconds from now
    while not time.time() > timeout:
        response_file_1 = test_app.get(f"/files/{post_response.json()[0]['id']}")
        response_file_2 = test_app.get(f"/files/{post_response.json()[1]['id']}")

        # TODO: Implement your condition check here. If it passes, break out of loop.
        if (
            response_file_1.status_code == 200
            and response_file_2.status_code == 200
            and response_file_1.json()['status'] == 'success'
            and response_file_2.json()['status'] == 'success'
        ):
            break

        time.sleep(1)  # Sleep for 1 second between checks
