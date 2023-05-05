def test_health(test_app):
    response = test_app.get('/health')
    assert response.status_code == 200
    assert response.json() == {'staging': True, 'status': 'ok'}


def test_authorized_user(test_app):
    response = test_app.get('/health/authorized_user', headers={'X-API-KEY': 'bar'})
    assert response.status_code == 200
    assert response.json() == {'authorized_user': True}

    response = test_app.get('/health/authorized_user', headers={'X-API-KEY': 'foo'})
    assert response.status_code == 403
    assert response.json() == {'detail': 'Bad API key credentials'}
