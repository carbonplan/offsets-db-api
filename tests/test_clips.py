def test_get_clips(test_app):
    response = test_app.get('/clips/')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)


def test_get_filtered_clips(test_app):
    response = test_app.get('/clips/?sort=-date&date_from=2024-01-01')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)
    data = response.json()['data']
    assert isinstance(data, list)
    entry = data[0]
    assert isinstance(entry['tags'], list)
