import datetime

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def sample_project(test_app):
    response = test_app.get('/projects')
    assert response.status_code == 200
    return response.json()['data'][0]


def test_get_projects_types(test_app: TestClient):
    response = test_app.get('/projects/types')
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0


def test_get_nonexistent_project(test_app: TestClient):
    response = test_app.get('/projects/123')
    assert response.status_code == 404
    assert response.json() == {'detail': 'project 123 not found'}


def test_get_existing_project(test_app: TestClient, sample_project):
    project_id = sample_project['project_id']
    response = test_app.get(f'/projects/{project_id}')
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert data['project_id'] == project_id
    assert data['registry'] == sample_project['registry']
    assert all(key in data for key in ['issued', 'retired', 'clips'])


def test_get_projects(test_app: TestClient):
    response = test_app.get('/projects/')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)
    assert len(data) > 1
    assert all(isinstance(project['clips'], list) for project in data)
    assert all(project['retired'] >= 0 for project in data)
    assert all(project['issued'] >= 0 for project in data)


@pytest.mark.parametrize('per_page, current_page', [(1, 1), (5, 2), (10, 3)])
def test_get_projects_pagination(test_app: TestClient, per_page, current_page):
    response = test_app.get(f'/projects?per_page={per_page}&current_page={current_page}')
    assert response.status_code == 200
    pagination = response.json()['pagination']
    assert isinstance(pagination, dict)
    assert pagination['current_page'] == current_page
    assert len(response.json()['data']) <= per_page


@pytest.mark.parametrize('registry', ['american-carbon-registry', 'climate-action-reserve'])
@pytest.mark.parametrize('country', ['US', 'CA'])
@pytest.mark.parametrize('protocol', [None, 'foo'])
@pytest.mark.parametrize('category', ['other'])
@pytest.mark.parametrize('listed_at_from', ['2020-01-01'])
@pytest.mark.parametrize('listed_at_to', ['2023-01-01'])
@pytest.mark.parametrize('search', ['foo'])
@pytest.mark.parametrize('retired_min,retired_max', [(0, 100000)])
@pytest.mark.parametrize('issued_min,issued_max', [(0, 100000)])
def test_get_projects_with_filters(
    test_app: TestClient,
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
    url = (
        f'/projects/?registry={registry}&country={country}&'
        f'protocol={protocol}&category={category}&'
        f'listed_at_from={listed_at_from}&listed_at_to={listed_at_to}&'
        f'search={search}&retired_min={retired_min}&retired_max={retired_max}&'
        f'issued_min={issued_min}&issued_max={issued_max}'
    )
    response = test_app.get(url)
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)
    for project in data:
        assert project['registry'] == registry
        assert project['country'] == country
        if protocol:
            assert project['protocol'] == protocol
        if category:
            assert category in project['category']
        assert retired_min <= project['retired'] <= retired_max
        assert issued_min <= project['issued'] <= issued_max


@pytest.mark.parametrize('beneficiary_search', ['foo'])
def test_projects_beneficiary_search(test_app: TestClient, beneficiary_search):
    response = test_app.get(f'/projects?beneficiary_search={beneficiary_search}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


def test_get_projects_with_invalid_sort(test_app: TestClient):
    response = test_app.get('/projects?sort=+foo')
    assert response.status_code == 400
    assert 'Invalid sort field:' in response.json()['detail']


def test_get_projects_with_valid_sort(test_app: TestClient):
    response = test_app.get('/projects?sort=+country&sort=project_id&sort=-listed_at')
    assert response.status_code == 200

    data = response.json()['data']
    assert isinstance(data, list)

    if data:
        prev_country, prev_project_id, prev_listed_at = None, None, None
        for project in data:
            country = project['country']
            project_id = project['project_id']
            listed_at = (
                datetime.datetime.fromisoformat(project['listed_at'])
                if project['listed_at']
                else None
            )

            if prev_country is not None:
                assert country >= prev_country, 'Projects are not sorted by country'
                if country == prev_country:
                    assert (
                        project_id >= prev_project_id
                    ), 'Projects are not sorted by project_id within the same country'
                    if project_id == prev_project_id and listed_at and prev_listed_at:
                        assert (
                            listed_at <= prev_listed_at
                        ), 'Projects are not sorted by listed_at within the same project_id'

            prev_country, prev_project_id, prev_listed_at = country, project_id, listed_at
