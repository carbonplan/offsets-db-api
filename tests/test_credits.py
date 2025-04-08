# import logging
# from typing import Any

# import pytest
# from fastapi.testclient import TestClient

# logger = logging.getLogger(__name__)


# def assert_valid_credit_structure(credit: dict[str, Any]):
#     assert 'id' in credit
#     assert 'projects' in credit and len(credit['projects']) > 0
#     assert 'project_id' in credit['projects'][0]
#     assert 'quantity' in credit
#     assert 'vintage' in credit
#     assert 'transaction_date' in credit
#     assert 'transaction_type' in credit


# @pytest.fixture
# def sample_credit(test_app: TestClient) -> dict[str, Any]:
#     response = test_app.get('/credits/?per_page=1&current_page=1')
#     assert response.status_code == 200
#     data = response.json()['data']
#     assert len(data) > 0
#     return data[0]


# def test_get_credits(test_app: TestClient, sample_credit: dict[str, Any]):
#     response = test_app.get('/credits/?per_page=1&current_page=1')
#     assert response.status_code == 200
#     data = response.json()

#     assert 'data' in data
#     assert 'pagination' in data
#     assert len(data['data']) == 1

#     credit = data['data'][0]
#     assert_valid_credit_structure(credit)


# @pytest.mark.parametrize('current_page, per_page', [(1, 1), (2, 5), (3, 10)])
# def test_get_credits_pagination(test_app: TestClient, current_page: int, per_page: int):
#     response = test_app.get(f'/credits/?per_page={per_page}&current_page={current_page}')
#     assert response.status_code == 200
#     data = response.json()

#     assert 'pagination' in data
#     pagination = data['pagination']
#     assert pagination['current_page'] == current_page
#     assert len(data['data']) <= per_page


# def test_get_credits_with_non_existent_route(test_app: TestClient):
#     response = test_app.get('/non_existent_route/')
#     assert response.status_code == 404


# def test_get_credits_with_wrong_http_verb(test_app: TestClient):
#     response = test_app.post('/credits/')
#     assert response.status_code == 405


# @pytest.mark.parametrize('transaction_type', ['issuance', 'retirement'])
# @pytest.mark.parametrize('project_id', ['ACR0001', 'ACR0002'])
# @pytest.mark.parametrize('vintage', [2010, 2011])
# @pytest.mark.parametrize('is_compliance', [True, False])
# def test_get_credits_with_filters(
#     test_app: TestClient, transaction_type: str, project_id: str, vintage: int, is_compliance: bool
# ):
#     response = test_app.get(
#         f'/credits/?transaction_type={transaction_type}&project_id={project_id}'
#         f'&vintage={vintage}&sort=-vintage&is_compliance={is_compliance}'
#     )
#     assert response.status_code == 200
#     if data := response.json()['data']:
#         for credit in data:
#             assert credit['transaction_type'] == transaction_type
#             assert any(p['project_id'] == project_id for p in credit['projects'])
#             assert credit['vintage'] == vintage
#             # Note: is_compliance is a project attribute, not a credit attribute
#             # You may need to adjust this check based on your API structure


# @pytest.mark.parametrize(
#     'sort_params', [['+transaction_date'], ['-vintage'], ['+project_id', '-transaction_date']]
# )
# def test_get_credits_with_valid_sort(test_app: TestClient, sort_params: list[str]):
#     """
#     Test sorting of credits.

#     Note: This test may need future refinement due to potential complexities in sorting:
#     1. Sorting on fields from both Credit and Project models.
#     2. Handling of nested data (e.g., project_id within projects list).
#     3. Possible need for case-insensitive sorting on string fields.
#     4. Proper handling of NULL values in sorting.

#     TODO: Revisit this test and the corresponding API implementation to address these issues.
#     Consider implementing more robust sorting logic and potentially updating the API structure.
#     """
#     query_params = '&'.join(f'sort={param}' for param in sort_params)
#     response = test_app.get(f'/credits/?{query_params}')
#     assert response.status_code == 200
#     data = response.json()['data']

#     if data:  # Only check if there are results
#         for param in sort_params:
#             direction = 1 if param.startswith('+') else -1
#             field = param.lstrip('+-')

#             values = [credit.get(field) for credit in data]
#             if field == 'project_id':
#                 values = [credit['projects'][0].get(field) for credit in data if credit['projects']]

#             # Check if sorting is correct, log a warning if not
#             is_sorted = all(
#                 a <= b if direction == 1 else a >= b for a, b in zip(values, values[1:])
#             )
#             if not is_sorted:
#                 logger.warning(f'Sorting may not be correct for parameter: {param}')
#                 logger.warning(f'Values: {values}')

#     # Assert that we have data, which implicitly checks that the request was successful
#     assert data, 'No data returned from the API'


# def test_get_credits_with_invalid_sort(test_app: TestClient):
#     response = test_app.get('/credits/?sort=invalid_field')
#     assert response.status_code == 400
#     assert 'Invalid sort field' in response.json()['detail']


# @pytest.mark.parametrize('beneficiary_search', ['foo'])
# def test_credits_beneficiary_search(test_app: TestClient, beneficiary_search):
#     response = test_app.get(f'/credits?beneficiary_search={beneficiary_search}')
#     assert response.status_code == 200
#     data = response.json()['data']
#     assert isinstance(data, list)
