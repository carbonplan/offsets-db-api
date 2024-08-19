from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def sample_clip(test_app: TestClient):
    response = test_app.get('/clips/?date_from=2000-01-01&per_page=1')
    assert response.status_code == 200
    data = response.json()['data']
    assert len(data) > 0
    return data[0]


def test_get_clips(test_app: TestClient, sample_clip):
    response = test_app.get('/clips/?date_from=2000-01-01')
    assert response.status_code == 200
    data = response.json()

    assert 'data' in data
    assert 'pagination' in data
    assert isinstance(data['data'], list)

    for clip in data['data']:
        assert_valid_clip_structure(clip)


def assert_valid_clip_structure(clip):
    assert 'id' in clip
    assert 'date' in clip
    assert 'title' in clip
    assert 'url' in clip
    assert 'source' in clip
    assert 'tags' in clip
    assert isinstance(clip['tags'], list)
    assert 'notes' in clip
    assert 'is_waybacked' in clip
    assert 'type' in clip
    assert 'projects' in clip
    assert isinstance(clip['projects'], list)

    for project in clip['projects']:
        assert 'project_id' in project
        assert project['project_id'] is not None
        assert 'category' in project
        assert project['category'] is not None


@pytest.mark.parametrize('current_page, per_page', [(1, 1), (2, 5), (3, 10)])
def test_get_clips_pagination(test_app: TestClient, current_page: int, per_page: int):
    response = test_app.get(
        f'/clips/?date_from=2000-01-01&current_page={current_page}&per_page={per_page}'
    )
    assert response.status_code == 200
    data = response.json()

    assert 'pagination' in data
    pagination = data['pagination']
    assert pagination['current_page'] == current_page
    assert len(data['data']) <= per_page


@pytest.mark.parametrize('source', ['The Guardian', 'Reuters'])
@pytest.mark.parametrize('clip_type', ['article', 'press release'])
def test_get_clips_with_filters(test_app: TestClient, source: str, clip_type: str):
    response = test_app.get(f'/clips/?source={source}&type={clip_type}')
    assert response.status_code == 200
    data = response.json()['data']

    for clip in data:
        assert clip['source'] == source
        assert clip['type'] == clip_type


def test_get_clips_with_date_range(test_app: TestClient):
    date_from = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    date_to = datetime.now().strftime('%Y-%m-%d')

    response = test_app.get(f'/clips/?date_from={date_from}&date_to={date_to}')
    assert response.status_code == 200
    data = response.json()['data']

    for clip in data:
        clip_date = datetime.fromisoformat(clip['date'])
        assert datetime.fromisoformat(date_from) <= clip_date <= datetime.fromisoformat(date_to)


@pytest.mark.parametrize('sort_params', [['+date'], ['-source'], ['+type', '-date']])
def test_get_clips_with_sort(test_app: TestClient, sort_params):
    query_params = '&'.join(f'sort={param}' for param in sort_params)
    response = test_app.get(f'/clips/?{query_params}')
    assert response.status_code == 200
    data = response.json()['data']

    if data:
        for param in sort_params:
            direction = 1 if param.startswith('+') else -1
            field = param.lstrip('+-')

            values = [clip[field] for clip in data]
            assert all(a <= b if direction == 1 else a >= b for a, b in zip(values, values[1:]))


def test_get_clips_with_invalid_sort(test_app: TestClient):
    response = test_app.get('/clips/?sort=invalid_field')
    assert response.status_code == 400
    assert 'Invalid sort field' in response.json()['detail']


def test_get_clips_with_search(test_app: TestClient):
    search_term = 'carbon'
    response = test_app.get(f'/clips/?search={search_term}')
    assert response.status_code == 200
    data = response.json()['data']

    for clip in data:
        assert search_term.lower() in clip['title'].lower() or any(
            search_term.lower() in project['project_id'].lower() for project in clip['projects']
        )
