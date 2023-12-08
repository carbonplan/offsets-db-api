def test_get_clips(test_app):
    response = test_app.get('/clips/')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)


def test_get_filtered_clips(test_app):
    response = test_app.get('/clips/')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)
    entry = response.json()['data'][0]
    response = test_app.get(
        f"/clips/?tags={entry['tags'][0]}&project_id={entry['projects'][0]['project_id']}&sort=-date"
    )
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)
    assert entry['tags'] == data[0]['tags']
    assert entry['projects'] == data[0]['projects']
