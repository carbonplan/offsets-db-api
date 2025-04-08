# from datetime import datetime, timedelta

# import pytest
# from fastapi.testclient import TestClient


# @pytest.mark.parametrize(
#     'url, category',
#     [
#         ('http://foo.com', 'projects'),
#         ('https://example.com', 'credits'),
#     ],
# )
# def test_submit_bad_file(test_app: TestClient, url: str, category: str):
#     response = test_app.post('/files', json=[{'url': url, 'category': category}])
#     assert response.status_code == 200
#     data = response.json()[0]
#     assert data['url'] == url
#     assert data['content_hash'] is None
#     assert data['status'] == 'pending'

#     file_response = test_app.get(f'/files/{data["id"]}')
#     assert file_response.json()['url'] == url


# @pytest.fixture
# def file_urls():
#     return [
#         {
#             'url': 's3://carbonplan-offsets-db/final/2025-03-06/credits-augmented.parquet',
#             'category': 'credits',
#         },
#         {
#             'url': 's3://carbonplan-offsets-db/final/2025-03-06/projects-augmented.parquet',
#             'category': 'projects',
#         },
#         {
#             'url': 's3://carbonplan-offsets-db/final/2025-03-06/curated-clips.parquet',
#             'category': 'clips',
#         },
#         {
#             'url': 's3://carbonplan-offsets-db/final/2025-01-21/weekly-summary-clips.parquet',
#             'category': 'clips',
#         },
#     ]


# def test_submit_file(test_app: TestClient, file_urls):
#     headers = {
#         'accept': 'application/json',
#         'Content-Type': 'application/json',
#     }
#     response = test_app.post('/files', headers=headers, json=file_urls)
#     assert response.status_code == 200
#     data = response.json()
#     assert isinstance(data, list)
#     assert len(data) == len(file_urls)

#     for submitted_file, url_data in zip(data, file_urls):
#         assert submitted_file['url'] == url_data['url']
#         assert submitted_file['content_hash'] is None
#         assert submitted_file['status'] == 'pending'

#     # Check the status of the first file after submission
#     file_response = test_app.get(f'/files/{data[0]["id"]}')
#     assert file_response.json()['url'] == file_urls[0]['url']
#     assert file_response.json()['status'] == 'success'


# def test_get_files(test_app: TestClient):
#     response = test_app.get('/files')
#     assert response.status_code == 200
#     data = response.json()
#     assert isinstance(data, dict)
#     assert 'data' in data
#     assert isinstance(data['data'], list)


# @pytest.mark.parametrize(
#     'current_page, per_page',
#     [
#         (1, 1),
#         (2, 5),
#         (3, 10),
#     ],
# )
# def test_get_files_pagination(test_app: TestClient, current_page: int, per_page: int):
#     response = test_app.get(f'/files?current_page={current_page}&per_page={per_page}')
#     assert response.status_code == 200
#     data = response.json()
#     assert isinstance(data, dict)
#     assert 'data' in data
#     assert isinstance(data['data'], list)
#     assert len(data['data']) <= per_page
#     assert data['pagination']['current_page'] == current_page


# @pytest.mark.parametrize(
#     'status, category',
#     [
#         ('success', 'projects'),
#         ('success', 'credits'),
#         ('success', 'clips'),
#     ],
# )
# def test_get_files_with_filters(test_app: TestClient, status: str, category: str):
#     recorded_at_from = (datetime.now() - timedelta(days=30)).isoformat()
#     recorded_at_to = (datetime.now() + timedelta(days=1)).isoformat()

#     response = test_app.get(
#         f'/files?status={status}&category={category}&recorded_at_from={recorded_at_from}&recorded_at_to={recorded_at_to}'
#     )
#     assert response.status_code == 200
#     data = response.json()
#     assert isinstance(data, dict)
#     assert 'data' in data
#     assert isinstance(data['data'], list)
#     assert len(data['data']) > 0

#     for file in data['data']:
#         assert file['status'] == status
#         assert file['category'] == category
#         recorded_at = datetime.fromisoformat(file['recorded_at'])
#         assert (
#             datetime.fromisoformat(recorded_at_from)
#             <= recorded_at
#             <= datetime.fromisoformat(recorded_at_to)
#         )
