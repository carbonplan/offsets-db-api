def test_get_clips(test_app):
    response = test_app.get('/clips/?date_from=2000-01-01')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)

    # For each clip, make sure category and project are present and not empty
    for clip in response.json()['data']:
        assert isinstance(clip['projects'], list)
        for project in clip['projects']:
            assert project['project_id'] is not None
            assert project['category'] is not None
