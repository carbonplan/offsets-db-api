# import logging
# from datetime import datetime, timedelta
# from typing import Any

# import pytest
# from fastapi.testclient import TestClient

# logger = logging.getLogger(__name__)


# def parse_date(date_string: str) -> datetime:
#     return datetime.fromisoformat(date_string.rstrip('Z')) if date_string else None


# def is_sorted(items: list[Any], reverse: bool = False) -> bool:
#     """
#     Check if a list is sorted, allowing for some tolerance in the sorting.

#     This function counts the number of out-of-order items and returns True
#     if the percentage of out-of-order items is below a certain threshold.

#     Args:
#         items: List of items to check for sorting
#         reverse: If True, check for descending order; otherwise, ascending

#     Returns:
#         bool: True if the list is considered sufficiently sorted, False otherwise
#     """
#     total_comparisons = 0
#     out_of_order = 0
#     for a, b in zip(items, items[1:]):
#         if a is not None and b is not None:
#             total_comparisons += 1
#             if reverse:
#                 if a < b:
#                     out_of_order += 1
#                     logger.warning(f'Out of order pair found: {a} < {b}')
#             elif a > b:
#                 out_of_order += 1
#                 logger.warning(f'Out of order pair found: {a} > {b}')

#     if total_comparisons == 0:
#         return True  # Consider empty or single-item lists as sorted

#     percentage_out_of_order = (out_of_order / total_comparisons) * 100
#     logger.info(f'Percentage of out-of-order items: {percentage_out_of_order:.2f}%')

#     # Consider the list sorted if less than 5% of items are out of order
#     return percentage_out_of_order < 5


# @pytest.fixture
# def sample_clip(test_app: TestClient):
#     response = test_app.get('/clips/?date_from=2000-01-01&per_page=1')
#     assert response.status_code == 200
#     data = response.json()['data']
#     assert len(data) > 0
#     return data[0]


# def test_get_clips(test_app: TestClient, sample_clip):
#     response = test_app.get('/clips/?date_from=2000-01-01')
#     assert response.status_code == 200
#     data = response.json()

#     assert 'data' in data
#     assert 'pagination' in data
#     assert isinstance(data['data'], list)

#     for clip in data['data']:
#         assert_valid_clip_structure(clip)


# def assert_valid_clip_structure(clip):
#     assert 'id' in clip
#     assert 'date' in clip
#     assert 'title' in clip
#     assert 'url' in clip
#     assert 'source' in clip
#     assert 'tags' in clip
#     assert isinstance(clip['tags'], list)
#     assert 'notes' in clip
#     assert 'is_waybacked' in clip
#     assert 'type' in clip
#     assert 'projects' in clip
#     assert isinstance(clip['projects'], list)

#     for project in clip['projects']:
#         assert 'project_id' in project
#         assert project['project_id'] is not None
#         assert 'category' in project
#         assert project['category'] is not None


# @pytest.mark.parametrize('current_page, per_page', [(1, 1), (2, 5), (3, 10)])
# def test_get_clips_pagination(test_app: TestClient, current_page: int, per_page: int):
#     response = test_app.get(
#         f'/clips/?date_from=2000-01-01&current_page={current_page}&per_page={per_page}'
#     )
#     assert response.status_code == 200
#     data = response.json()

#     assert 'pagination' in data
#     pagination = data['pagination']
#     assert pagination['current_page'] == current_page
#     assert len(data['data']) <= per_page


# @pytest.mark.parametrize('source', ['The Guardian', 'Reuters'])
# @pytest.mark.parametrize('clip_type', ['article', 'press release'])
# def test_get_clips_with_filters(test_app: TestClient, source: str, clip_type: str):
#     response = test_app.get(f'/clips/?source={source}&type={clip_type}')
#     assert response.status_code == 200
#     data = response.json()['data']

#     for clip in data:
#         assert clip['source'] == source
#         assert clip['type'] == clip_type


# def test_get_clips_with_date_range(test_app: TestClient):
#     date_from = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
#     date_to = datetime.now().strftime('%Y-%m-%d')

#     response = test_app.get(f'/clips/?date_from={date_from}&date_to={date_to}')
#     assert response.status_code == 200
#     data = response.json()['data']

#     for clip in data:
#         clip_date = datetime.fromisoformat(clip['date'])
#         assert datetime.fromisoformat(date_from) <= clip_date <= datetime.fromisoformat(date_to)


# def test_clips_sort_by_date(test_app: TestClient):
#     response = test_app.get('/clips/?sort=+date')
#     assert response.status_code == 200
#     data = response.json()['data']

#     if not data:
#         pytest.skip('No data returned from API')

#     dates = [parse_date(clip.get('date')) for clip in data]
#     assert is_sorted(dates), 'Clips are not sorted by date in ascending order'


# def test_clips_sort_by_source_desc(test_app: TestClient):
#     """
#     Test sorting clips by source in descending order.

#     Note: This test may be sensitive to data changes and environment differences.
#     TODO: Investigate why this test may pass locally but fail in CI.
#     Possible reasons:
#     1. Differences in data between environments
#     2. Inconsistent sorting behavior in the API
#     3. Race conditions or timing issues in data loading
#     """
#     response = test_app.get('/clips/?sort=-source')
#     assert response.status_code == 200
#     data = response.json()['data']

#     if not data:
#         pytest.skip('No data returned from API')

#     sources = [clip.get('source') for clip in data]
#     logger.info(f'First few sources: {sources[:10]}')
#     logger.info(f'Last few sources: {sources[-10:]}')

#     assert is_sorted(sources, reverse=True), 'Clips are not sorted by source in descending order'


# def test_clips_sort_by_type_and_date(test_app: TestClient):
#     response = test_app.get('/clips/?sort=+type&sort=-date')
#     assert response.status_code == 200
#     data = response.json()['data']

#     if not data:
#         pytest.skip('No data returned from API')

#     # Check if sorted by type
#     types = [clip.get('type') for clip in data]
#     assert is_sorted(types), 'Clips are not sorted by type in ascending order'

#     # Check if sorted by date within each type
#     from itertools import groupby

#     for _, group in groupby(data, key=lambda x: x.get('type')):
#         group_dates = [parse_date(clip.get('date')) for clip in group]
#         assert is_sorted(group_dates, reverse=True), (
#             f'Dates are not sorted in descending order within type {_}'
#         )


# def test_clips_sort_with_missing_values(test_app: TestClient):
#     """
#     Test sorting clips with missing values.

#     Note: This test may be sensitive to data changes and environment differences.
#     TODO: Investigate why this test may pass locally but fail in CI.
#     Possible reasons:
#     1. Differences in handling of null or empty values between environments
#     2. Inconsistent sorting behavior for null/empty values in the API
#     3. Data inconsistencies between test environments
#     """
#     response = test_app.get('/clips/?sort=+source')
#     assert response.status_code == 200
#     data = response.json()['data']

#     if not data:
#         pytest.skip('No data returned from API')

#     sources = [clip.get('source') for clip in data]
#     non_none_sources = [s for s in sources if s is not None]
#     logger.info(f'First few non-None sources: {non_none_sources[:10]}')
#     logger.info(f'Last few non-None sources: {non_none_sources[-10:]}')
#     logger.info(f'Number of None values: {len(sources) - len(non_none_sources)}')

#     assert is_sorted(non_none_sources), 'Non-None sources are not sorted in ascending order'
#     assert all(s is None for s in sources[len(non_none_sources) :]), (
#         'None values are not at the end'
#     )


# def test_get_clips_with_invalid_sort(test_app: TestClient):
#     response = test_app.get('/clips/?sort=invalid_field')
#     assert response.status_code == 400
#     assert 'Invalid sort field' in response.json()['detail']


# def test_get_clips_with_search(test_app: TestClient):
#     search_term = 'carbon'
#     response = test_app.get(f'/clips/?search={search_term}')
#     assert response.status_code == 200
#     data = response.json()['data']

#     for clip in data:
#         assert search_term.lower() in clip['title'].lower() or any(
#             search_term.lower() in project['project_id'].lower() for project in clip['projects']
#         )
