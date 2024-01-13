def test_health(test_app):
    response = test_app.get('/health/database')
    assert response.status_code == 200
    data = response.json()
    assert data.keys() == {'status', 'staging', 'latest-successful-db-update'}
    assert data['status'] == 'ok'
    assert data['staging'] is True
    assert data['latest-successful-db-update'].keys() == {'projects', 'credits', 'clips'}
    assert response.headers['x-offsetsdb-cache'] == 'MISS'
    assert 'cache-control' in response.headers
    assert 'etag' in response.headers

    etag = response.headers.get('etag')
    response = test_app.get('/health/database', headers={'If-None-Match': etag})
    assert response.status_code == 304
    assert response.headers['x-offsetsdb-cache'] == 'HIT'


def test_authorized_user(test_app):
    response = test_app.get('/health/authorized_user', headers={'X-API-KEY': 'cowsay'})
    assert response.status_code == 200
    assert response.json() == {'authorized_user': True}


def test_unauthorized_user(test_app):
    response = test_app.get('/health/authorized_user', headers={'X-API-KEY': 'foo'})
    assert response.status_code == 403
    assert 'Bad API key credentials' in response.json()['detail']


def test_missing_api_key(test_app):
    headers = test_app.headers
    test_app.headers = {}
    response = test_app.get('/health/authorized_user')
    assert response.status_code == 403
    assert 'Missing API key' in response.json()['detail']
    test_app.headers = headers
