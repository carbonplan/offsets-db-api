import datetime
import time

import pytest


def test_submit_bad_filea(test_app):
    response = test_app.post('/projects/files', json=[{'url': 'http://foo.com'}])
    assert response.status_code == 200
    data = response.json()[0]
    assert data['url'] == 'http://foo.com'
    assert data['content_hash'] is None
    assert data['status'] == 'pending'

    response = test_app.get(f"/projects/files/{data['id']}")
    assert response.json()['url'] == 'http://foo.com'


@pytest.mark.parametrize(
    'urls',
    [
        [
            {
                'url': 's3://carbonplan-share/offsets-db-testing-data/data/processed/latest/american-carbon-registry/projects.csv.gz'
            }
        ],
    ],
)
def test_submit_file(test_app, urls):
    response = test_app.post('/projects/files', json=urls)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == len(urls)
    for i, url in enumerate(urls):
        assert data[i]['url'] == url['url']
        assert data[i]['content_hash'] is None
        assert data[i]['status'] == 'pending'

    time.sleep(2)
    response = test_app.get(f"/projects/files/{response.json()[0]['id']}")
    assert response.json()['url'] == urls[0]['url']
    assert response.json()['status'] == 'success'
    assert response.json()['content_hash'] is not None


def test_get_files(test_app):
    response = test_app.get('/projects/files')
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_files_limit_offset(test_app):
    response = test_app.get('/projects/files?limit=1&offset=1')
    assert response.status_code == 200
    assert len(response.json()) == 1


def test_get_files_with_filters(test_app):
    recorded_at_from = datetime.datetime.now() - datetime.timedelta(days=30)
    recorded_at_to = datetime.datetime.now() + datetime.timedelta(days=1)
    response = test_app.get(
        f'/projects/files?status=success&category=projects&recorded_at_from={recorded_at_from.isoformat()}&recorded_at_to={recorded_at_to.isoformat()}'
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


def test_get_project(test_app):
    response = test_app.get('/projects/123')
    assert response.status_code == 404
    assert response.json() == {'detail': 'project 123 not found'}


def test_get_projects(test_app):
    response = test_app.get('/projects')
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_projects_limit_offset(test_app):
    response = test_app.get('/projects?limit=1&offset=1')
    assert response.status_code == 200
    assert len(response.json()) == 1


@pytest.mark.parametrize('registry', ['american-carbon-registry', 'climate-action-reserve'])
@pytest.mark.parametrize('country', ['US', 'CA'])
@pytest.mark.parametrize('protocol', [None, 'foo'])
def test_get_projects_with_filters(test_app, registry, country, protocol):
    response = test_app.get(f'/projects?registry={registry}&country={country}&protocol={protocol}')
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        for project in data:
            assert project['registry'] == registry
            assert project['country'] == country
            if protocol:
                assert project['protocol'] == protocol
