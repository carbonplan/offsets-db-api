import datetime
import json
import time


def test_submit_bad_file(test_app):
    response = test_app.post('/files', json=[{'url': 'http://foo.com', 'category': 'projects'}])
    assert response.status_code == 200
    data = response.json()[0]
    assert data['url'] == 'http://foo.com'
    assert data['content_hash'] is None
    assert data['status'] == 'pending'

    response = test_app.get(f"/files/{data['id']}")
    assert response.json()['url'] == 'http://foo.com'


def test_submit_file(test_app):
    urls = [
        {
            'url': 's3://carbonplan-offsets-db/final/2024-02-08/credits-augmented.parquet',
            'category': 'credits',
        },
        {
            'url': 's3://carbonplan-offsets-db/final/2024-02-08/projects-augmented.parquet',
            'category': 'projects',
        },
        {
            'url': 's3://carbonplan-offsets-db/final/2024-02-08/curated-clips.parquet',
            'category': 'clips',
        },
        # {
        #     'url': 's3://carbonplan-offsets-db/final/2024-02-13/weekly-summary-clips.parquet',
        #     'category': 'clips',
        # },
    ]

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }
    response = test_app.post('/files', headers=headers, data=json.dumps(urls))
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == len(urls)
    for i, url in enumerate(urls):
        assert data[i]['url'] == url['url']
        assert data[i]['content_hash'] is None
        assert data[i]['status'] == 'pending'

    time.sleep(2)
    response = test_app.get(f"/files/{response.json()[0]['id']}")
    assert response.json()['url'] == urls[0]['url']
    assert response.json()['status'] == 'success'


def test_get_files(test_app):
    response = test_app.get('/files')
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_files_limit_offset(test_app):
    response = test_app.get('/files?limit=1&offset=1')
    assert response.status_code == 200
    assert len(response.json()) == 1


def test_get_files_with_filters(test_app):
    recorded_at_from = datetime.datetime.now() - datetime.timedelta(days=30)
    recorded_at_to = datetime.datetime.now() + datetime.timedelta(days=1)
    response = test_app.get(
        f'/files?status=success&category=projects&recorded_at_from={recorded_at_from.isoformat()}&recorded_at_to={recorded_at_to.isoformat()}'
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) > 0
    for file in response.json():
        assert file['status'] == 'success'
        assert file['category'] == 'projects'
        recorded_at = datetime.datetime.fromisoformat(file['recorded_at'])
        assert recorded_at >= recorded_at_from
        assert recorded_at <= recorded_at_to
