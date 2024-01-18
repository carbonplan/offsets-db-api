def test_get_clips(test_app):
    response = test_app.get('/clips/')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)

    # For each clip, make sure category and project are present and not empty
    for clip in response.json()['data']:
        assert isinstance(clip['projects'], list)
        for project in clip['projects']:
            assert project['project_id'] is not None
            assert project['category'] is not None


def test_get_filtered_clips(test_app):
    response = test_app.get('/clips/?sort=-date&date_from=2024-01-01')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)
    data = response.json()['data']
    assert isinstance(data, list)
    entry = data[0]
    assert isinstance(entry['tags'], list)
