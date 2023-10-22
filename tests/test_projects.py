import datetime

import pytest


def test_get_project(test_app):
    response = test_app.get('/projects/123')
    assert response.status_code == 404
    assert response.json() == {'detail': 'project 123 not found'}

    response = test_app.get('/projects')
    assert response.status_code == 200
    data = response.json()['data'][0]
    project_id = data['project_id']
    registry = data['registry']
    assert isinstance(data['clips'], list)

    response = test_app.get(f'/projects/{project_id}')
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert data['project_id'] == project_id
    assert data['registry'] == registry

    assert 'issued' in data
    assert 'retired' in data


def test_get_projects(test_app):
    response = test_app.get('/projects/')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)


def test_get_projects_pagination(test_app):
    response = test_app.get('/projects?per_page=1&current_page=1')
    assert response.status_code == 200
    pagination = response.json()['pagination']
    assert isinstance(pagination, dict)
    assert pagination['current_page'] == 1
    assert len(response.json()['data']) == 1


@pytest.mark.parametrize('registry', ['american-carbon-registry', 'climate-action-reserve'])
@pytest.mark.parametrize('country', ['US', 'CA'])
@pytest.mark.parametrize('protocol', [None, 'foo'])
@pytest.mark.parametrize('category', ['other'])
@pytest.mark.parametrize('listed_at_from', ['2020-01-01'])
@pytest.mark.parametrize('listed_at_to', ['2023-01-01'])
@pytest.mark.parametrize('search', ['foo'])
@pytest.mark.parametrize('retired_min', [0])
@pytest.mark.parametrize('retired_max', [100000])
@pytest.mark.parametrize('issued_min', [0])
@pytest.mark.parametrize('issued_max', [100000])
def test_get_projects_with_filters(
    test_app,
    registry,
    country,
    protocol,
    category,
    listed_at_from,
    listed_at_to,
    search,
    retired_min,
    retired_max,
    issued_min,
    issued_max,
):
    response = test_app.get(
        f'/projects?registry={registry}&country={country}&protocol={protocol}&category={category}&listed_at_from={listed_at_from}&listed_at_to={listed_at_to}&search={search}&retired_min={retired_min}&retired_max={retired_max}&issued_min={issued_min}&issued_max={issued_max}'
    )
    assert response.status_code == 200
    data = response.json()['data']
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
    response = test_app.get('/projects?sort=+country&sort=project_id&sort=-listed_at')

    # Assert that the request was successful
    assert response.status_code == 200

    # Parse the JSON response
    data = response.json()['data']

    # Assert that the response is a list
    assert isinstance(data, list), f'Expected List, got {type(data).__name__}'

    # If there are any projects in the response, check that they are sorted
    if data:
        prev_country, prev_project_id, prev_listed_at = None, None, None
        for project in data:
            country = project['country']
            project_id = project['project_id']
            listed_at_str = project['listed_at']
            listed_at = (
                datetime.datetime.strptime(listed_at_str, '%Y-%m-%d') if listed_at_str else None
            )

            # Check the sorting logic
            if prev_country is not None and country is not None:
                assert country >= prev_country, 'Projects are not sorted by country'
                if country == prev_country:
                    assert (
                        project_id >= prev_project_id
                    ), 'Projects are not sorted by project_id within the same country'
                    if project_id == prev_project_id:
                        assert (
                            listed_at <= prev_listed_at
                        ), 'Projects are not sorted by listed_at within the same project_id'

            prev_country, prev_project_id, prev_listed_at = country, project_id, listed_at
