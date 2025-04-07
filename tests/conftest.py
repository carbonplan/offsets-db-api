import logging
import time

import pytest
from fastapi.testclient import TestClient

from offsets_db_api.database import get_session
from offsets_db_api.main import create_application
from offsets_db_api.settings import Settings, get_settings

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_settings_override():
    return Settings(staging=True, api_key='cowsay')


@pytest.fixture(scope='function')
def test_db_session():
    session = get_session()
    yield session
    session.close()


@pytest.fixture(scope='session')
def test_app():
    app = create_application()
    app.dependency_overrides[get_settings] = get_settings_override
    headers = {'X-API-Key': 'cowsay'}
    with TestClient(app) as test_client:
        test_client.headers.update(headers)
        yield test_client


def wait_for_file_processing(test_app: TestClient, file_ids: list[str], timeout: int = 60) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout:
        all_processed = all(
            test_app.get(f'/files/{file_id}').json()['status'] == 'success' for file_id in file_ids
        )
        if all_processed:
            return True
        time.sleep(2)  # Increased sleep time to reduce API calls
    return False


@pytest.fixture(scope='session', autouse=True)
def setup_post(test_app: TestClient):
    payload: list[dict[str, str]] = [
        {
            'url': 's3://carbonplan-offsets-db/final/2025-03-06/credits-augmented.parquet',
            'category': 'credits',
        },
        {
            'url': 's3://carbonplan-offsets-db/final/2025-03-06/projects-augmented.parquet',
            'category': 'projects',
        },
        {
            'url': 's3://carbonplan-offsets-db/final/2025-03-06/curated-clips.parquet',
            'category': 'clips',
        },
        {
            'url': 's3://carbonplan-offsets-db/final/2025-01-21/weekly-summary-clips.parquet',
            'category': 'clips',
        },
    ]

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    try:
        post_response = test_app.post('/files', headers=headers, json=payload)
        post_response.raise_for_status()
        logger.info(f'POST response: {post_response.json()}')

        file_ids = [file['id'] for file in post_response.json()]
        if wait_for_file_processing(test_app, file_ids):
            logger.info('All files processed successfully')
        else:
            logger.error('Timeout: Not all files were processed')
            pytest.fail('File processing timeout')
    except Exception as e:
        logger.error(f'Error during file setup: {str(e)}')
        pytest.fail(f'File setup failed: {str(e)}')
