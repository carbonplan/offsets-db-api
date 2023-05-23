import datetime
import time

import pytest


def test_submit_bad_file(test_app):
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
@pytest.mark.parametrize('category', ['other'])
@pytest.mark.parametrize('started_at_from', ['2020-01-01'])
@pytest.mark.parametrize('started_at_to', ['2023-01-01'])
@pytest.mark.parametrize('search', ['foo'])
def test_get_projects_with_filters(
    test_app, registry, country, protocol, category, started_at_from, started_at_to, search
):
    response = test_app.get(
        f'/projects?registry={registry}&country={country}&protocol={protocol}&category={category}&started_at_from={started_at_from}&started_at_to={started_at_to}&search={search}'
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        for project in data:
            assert project['registry'] == registry
            assert project['country'] == country
            if protocol:
                assert project['protocol'] == protocol
            if category:
                assert project['category'] == category


def test_get_projects_with_sort_errors(test_app):
    # Request sorted data from the endpoint
    response = test_app.get('/projects?sort=+foo')

    # Assert that the request was successful
    assert response.status_code == 400

    # Parse the JSON response
    data = response.json()

    # Assert that the response is a list
    assert isinstance(data, dict), f'Expected Dict, got {type(data).__name__}'

    # Assert that the error message is correct
    assert 'Invalid sort field:' in data['detail']


def test_get_projects_with_sort(test_app):
    # Request sorted data from the endpoint
    response = test_app.get('/projects?sort=+country&sort=project_id&sort=-registered_at')

    # Assert that the request was successful
    assert response.status_code == 200

    # Parse the JSON response
    data = response.json()

    # Assert that the response is a list
    assert isinstance(data, list), f'Expected List, got {type(data).__name__}'

    # If there are any projects in the response, check that they are sorted
    if data:
        prev_country, prev_project_id, prev_registered_at = None, None, None
        for project in data:
            country = project['country']
            project_id = project['project_id']
            registered_at_str = project['registered_at']
            registered_at = (
                datetime.datetime.strptime(registered_at_str, '%Y-%m-%d')
                if registered_at_str
                else None
            )

            # Check the sorting logic
            if prev_country is not None:
                assert country >= prev_country, 'Projects are not sorted by country'
                if country == prev_country:
                    assert (
                        project_id >= prev_project_id
                    ), 'Projects are not sorted by project_id within the same country'
                    if project_id == prev_project_id:
                        assert (
                            registered_at <= prev_registered_at
                        ), 'Projects are not sorted by registered_at within the same project_id'

            prev_country, prev_project_id, prev_registered_at = country, project_id, registered_at
