import pytest
from fastapi.testclient import TestClient


class TestHealthEndpoint:
    def test_health_response_structure(self, test_app: TestClient):
        """Test the structure and content of the health endpoint response."""
        response = test_app.get('/health/database')
        assert response.status_code == 200
        data = response.json()

        expected_keys = {
            'status',
            'staging',
            'latest-successful-db-update',
            'database-pool-size',
            'web-concurrency',
        }
        assert set(data.keys()) == expected_keys
        assert data['status'] == 'ok'
        assert data['staging'] is True
        assert set(data['latest-successful-db-update'].keys()) == {'projects', 'credits', 'clips'}

    def test_health_response_headers(self, test_app: TestClient):
        """Test the headers of the health endpoint response."""
        response = test_app.get('/health/database')
        assert 'cache-control' in response.headers
        assert 'etag' in response.headers

    def test_health_etag_caching(self, test_app: TestClient):
        """Test the ETag-based caching of the health endpoint."""
        initial_response = test_app.get('/health/database')
        etag = initial_response.headers.get('etag')

        cached_response = test_app.get('/health/database', headers={'If-None-Match': etag})
        assert cached_response.status_code == 304


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
        del test_app.headers['X-API-Key']
        response = test_app.get(auth_endpoint, headers={})
        assert response.status_code == 403
        assert response.json() == {
            'detail': 'Missing API key. Please provide one in the X-API-KEY header.'
        }
