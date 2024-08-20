from datetime import datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient


def parse_date(date_string: str) -> datetime:
    return datetime.fromisoformat(date_string.rstrip('Z')) if date_string else None


def is_sorted(items: list[Any], reverse: bool = False) -> bool:
    return all(
        a >= b if reverse else a <= b
        for a, b in zip(items, items[1:])
        if a is not None and b is not None
    )


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
def test_get_clips_with_sort(test_app: TestClient, sort_params: list[str]):
    import itertools

    query_params = '&'.join(f'sort={param}' for param in sort_params)
    response = test_app.get(f'/clips/?{query_params}')
    assert response.status_code == 200
    data = response.json()['data']

    if not data:
        pytest.skip('No data returned from API')

    print(f'\nSorting by parameters: {sort_params}')

    for i, param in enumerate(sort_params):
        direction = 1 if param.startswith('+') else -1
        field = param.lstrip('+-')

        if field == 'date':
            values = [parse_date(clip.get(field)) for clip in data]
        else:
            values = [clip.get(field) for clip in data]

        if i == 0:  # First sort parameter
            is_correct_order = is_sorted(values, reverse=(direction == -1))
            print(f"\nSorting by {field} ({'ascending' if direction == 1 else 'descending'}):")
            for value in values:
                print(f'  {value}')
        else:  # Subsequent sort parameters
            # Group by all previous sort fields
            group_fields = [p.lstrip('+-') for p in sort_params[:i]]
            groups = itertools.groupby(data, key=lambda x: tuple(x.get(f) for f in group_fields))

            for group_key, group_data in groups:
                group_values = [
                    parse_date(item[field]) if field == 'date' else item.get(field)
                    for item in group_data
                ]
                is_correct_order = is_sorted(group_values, reverse=(direction == -1))
                print(
                    f"\nGroup {group_key} sorted by {field} ({'ascending' if direction == 1 else 'descending'}):"
                )
                for value in group_values:
                    print(f'  {value}')
                if not is_correct_order:
                    break

        assert is_correct_order, f'Sorting by {field} failed'

    print(f'\nSorting successful for parameters: {sort_params}')


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
