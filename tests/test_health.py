from datetime import datetime
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


class TestAuthorizationEndpoint:
    @pytest.fixture
    def auth_endpoint(self):
        return '/health/authorized_user'

    def test_authorized_user(self, test_app: TestClient, auth_endpoint):
        """Test the response for an authorized user."""
        response = test_app.get(auth_endpoint, headers={'X-API-KEY': 'cowsay'})
        assert response.status_code == 200
        assert response.json() == {'authorized_user': True}

    def test_unauthorized_user(self, test_app: TestClient, auth_endpoint):
        """Test the response for an unauthorized user."""
        response = test_app.get(auth_endpoint, headers={'X-API-KEY': 'foo'})
        assert response.status_code == 403
        assert 'Bad API key credentials' in response.json()['detail']

    def test_missing_api_key(self, test_app: TestClient, auth_endpoint):
        """Test the response when the API key is missing."""

        original_headers = test_app.headers.copy()
        del test_app.headers['X-API-Key']
        response = test_app.get(auth_endpoint, headers={})
        # Restore the original headers
        test_app.headers = original_headers

        assert response.status_code == 403
        assert response.json() == {
            'detail': 'Missing API key. Please provide one in the X-API-KEY header.'
        }


class TestHealthEndpoint:
    def test_health_endpoint(self, test_app: TestClient, monkeypatch):
        """Test the root health endpoint."""
        # Mock settings
        mock_settings = MagicMock()
        mock_settings.staging = False
        monkeypatch.setattr('offsets_db_api.routers.health.get_settings', lambda: mock_settings)

        response = test_app.get('/health/')
        assert response.status_code == 200

    def test_health_endpoint_staging(self, test_app: TestClient, monkeypatch):
        """Test the root health endpoint with staging enabled."""
        # Mock settings
        mock_settings = MagicMock()
        mock_settings.staging = True
        monkeypatch.setattr('offsets_db_api.routers.health.get_settings', lambda: mock_settings)

        response = test_app.get('/health/')
        assert response.status_code == 200

    def test_health_response_headers(self, test_app: TestClient):
        """Test the headers of the health endpoint response."""
        response = test_app.get('/health/database')
        assert 'cache-control' in response.headers
        assert 'etag' in response.headers


class TestDBStatusEndpoint:
    @pytest.mark.xfail(reason='Needs to be fixed')
    def test_db_status_with_mocked_db(self, test_app: TestClient, monkeypatch):
        """Test the database status endpoint with mocked database results."""
        # Mock session and query results
        mock_session = MagicMock()
        mock_results = [
            (
                MagicMock(value='projects'),
                datetime(2023, 1, 1, 12, 0),
                'http://example.com/projects',
            ),
            (MagicMock(value='credits'), datetime(2023, 1, 2, 12, 0), 'http://example.com/credits'),
            (MagicMock(value='clips'), datetime(2023, 1, 3, 12, 0), 'http://example.com/clips'),
        ]
        mock_session.exec.return_value.all.return_value = mock_results

        # Mock settings
        mock_settings = MagicMock()
        mock_settings.staging = True
        mock_settings.database_pool_size = 5
        mock_settings.web_concurrency = 4

        # Apply mocks
        monkeypatch.setattr('offsets_db_api.routers.health.get_session', lambda: mock_session)
        monkeypatch.setattr('offsets_db_api.routers.health.get_settings', lambda: mock_settings)

        # Disable caching for this test
        monkeypatch.setattr(
            'fastapi_cache.decorator.cache', lambda *args, **kwargs: lambda func: func
        )

        response = test_app.get('/health/database')
        assert response.status_code == 200
        data = response.json()

        # Check structure
        assert 'status' in data
        assert 'staging' in data
        assert 'database-pool-size' in data
        assert 'web-concurrency' in data
        assert 'latest-successful-db-update' in data

        # Check values
        assert data['status'] == 'ok'
        assert isinstance(data['staging'], bool)
        assert isinstance(data['database-pool-size'], int)
        assert isinstance(data['web-concurrency'], int)

        # Check latest updates
        updates = data['latest-successful-db-update']
        assert 'projects' in updates
        assert 'credits' in updates

    def test_health_etag_caching(self, test_app: TestClient):
        """Test the ETag-based caching of the health endpoint."""
        initial_response = test_app.get('/health/database')
        etag = initial_response.headers.get('etag')

        cached_response = test_app.get('/health/database', headers={'If-None-Match': etag})
        assert cached_response.status_code == 304

    @pytest.mark.xfail(reason='Needs to be fixed')
    def test_db_status_partial_results(self, test_app: TestClient, monkeypatch):
        """Test database status endpoint with partial results."""
        # Only projects and credits, no clips
        mock_session = MagicMock()
        mock_results = [
            (
                MagicMock(value='projects'),
                datetime(2023, 1, 1, 12, 0),
                'http://example.com/projects',
            ),
            (MagicMock(value='credits'), datetime(2023, 1, 2, 12, 0), 'http://example.com/credits'),
        ]
        mock_session.exec.return_value.all.return_value = mock_results

        mock_settings = MagicMock()
        mock_settings.staging = True
        mock_settings.database_pool_size = 5
        mock_settings.web_concurrency = 4

        monkeypatch.setattr('offsets_db_api.routers.health.get_session', lambda: mock_session)
        monkeypatch.setattr('offsets_db_api.routers.health.get_settings', lambda: mock_settings)
        monkeypatch.setattr(
            'fastapi_cache.decorator.cache', lambda *args, **kwargs: lambda func: func
        )

        response = test_app.get('/health/database')
        assert response.status_code == 200
        data = response.json()

        updates = data['latest-successful-db-update']
        assert 'projects' in updates
        assert 'credits' in updates
        # assert 'clips' not in updates
