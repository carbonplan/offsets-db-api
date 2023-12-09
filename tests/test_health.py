def test_health(test_app):
    response = test_app.get('/health')
    assert response.status_code == 200
    assert response.json() == {'staging': True, 'status': 'ok'}


def test_authorized_user(test_app):
    response = test_app.get('/health/authorized_user', headers={'X-API-KEY': 'cowsay'})
    assert response.status_code == 200
    assert response.json() == {'authorized_user': True}


def test_unauthorized_user(test_app):
    response = test_app.get('/health/authorized_user', headers={'X-API-KEY': 'foo'})
    assert response.status_code == 403
    assert 'Bad API key credentials' in response.json()['detail']


def test_missing_api_key(test_app):
    test_app.headers = {}
    response = test_app.get('/health/authorized_user')
    assert response.status_code == 403
    assert 'Missing API key' in response.json()['detail']
