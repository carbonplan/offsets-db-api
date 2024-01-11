def test_health(test_app):
    response = test_app.get('/health/database')
    assert response.status_code == 200
    data = response.json()
    assert data.keys() == {'status', 'staging', 'latest-successful-db-update'}
    assert data['status'] == 'ok'
    assert data['staging'] is True
    assert data['latest-successful-db-update'].keys() == {'projects', 'credits', 'clips'}


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
