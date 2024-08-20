import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def sample_projects():
    return pd.DataFrame(
        [
            {'category': 'ghg-management', 'project_id': 'ACR123', 'issued': 100, 'retired': 50},
            {'category': 'renewable-energy', 'project_id': 'ACR456', 'issued': 200, 'retired': 150},
            {'category': 'biodiversity', 'project_id': 'VER789', 'issued': 300, 'retired': 250},
            {
                'category': 'water-management',
                'project_id': 'CDM1011',
                'issued': 400,
                'retired': 350,
            },
            {'category': 'ghg-management', 'project_id': 'ACR111', 'issued': 500, 'retired': 450},
        ]
    )


def generate_query_string(params: dict) -> str:
    return '&'.join(f'{k}={v}' for k, v in params.items() if v is not None)


@pytest.fixture
def common_params():
    return {
        'registry': 'american-carbon-registry',
        'country': 'US',
        'protocol': 'foo',
        'category': 'other',
        'listed_at_from': '2020-01-01',
        'listed_at_to': '2023-01-01',
        'search': 'foo',
        'retired_min': '0',
        'retired_max': '100000',
        'issued_min': '0',
        'issued_max': '100000',
        'is_compliance': 'true',
    }


@pytest.mark.parametrize('freq', ['D', 'M', 'Y', 'W'])
def test_get_projects_by_listing_date(
    test_app: TestClient, freq: str, common_params: dict[str, str]
):
    """Test the projects_by_listing_date endpoint with various parameters."""
    query_params = {**common_params, 'freq': freq}
    query_string = generate_query_string(query_params)
    response = test_app.get(f'/charts/projects_by_listing_date?{query_string}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('freq', ['D', 'M', 'Y', 'W'])
def test_get_credits_by_transaction_date(
    test_app: TestClient, freq: str, common_params: dict[str, str]
):
    """Test the credits_by_transaction_date endpoint with various parameters."""
    query_params = {**common_params, 'freq': freq}
    query_string = generate_query_string(query_params)
    response = test_app.get(f'/charts/credits_by_transaction_date?{query_string}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('num_bins', [5, 20, 30])
@pytest.mark.parametrize('transaction_type', ['issuance', 'retirement'])
@pytest.mark.parametrize('vintage', [2015, 2020])
@pytest.mark.parametrize('transaction_date_from', ['2020-01-01'])
@pytest.mark.parametrize('transaction_date_to', ['2023-01-01', '2024-01-01'])
def test_get_credits_by_transaction_date_by_project(
    test_app: TestClient,
    num_bins: int,
    transaction_type: str,
    vintage: int,
    transaction_date_from: str,
    transaction_date_to: str,
):
    """Test the credits_by_transaction_date endpoint for a specific project."""
    project_id = 'ACR462'
    query_params = {
        'num_bins': str(num_bins),
        'transaction_type': transaction_type,
        'vintage': str(vintage),
        'transaction_date_from': transaction_date_from,
        'transaction_date_to': transaction_date_to,
    }
    query_string = generate_query_string(query_params)
    response = test_app.get(f'/charts/credits_by_transaction_date/{project_id}/?{query_string}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


def test_get_credits_by_transaction_date_by_nonexistent_project(test_app: TestClient):
    """Test the credits_by_transaction_date endpoint for a nonexistent project."""
    project_id = 'ACR999'
    response = test_app.get(f'/charts/credits_by_transaction_date/{project_id}/')
    assert response.status_code == 200
    assert response.json()['data'] == []


@pytest.mark.parametrize('credit_type', ['issued', 'retired'])
def test_get_projects_by_credit_totals(
    test_app: TestClient, credit_type: str, common_params: dict[str, str]
):
    """Test the projects_by_credit_totals endpoint with various parameters."""
    query_params = {**common_params, 'credit_type': credit_type}
    query_string = generate_query_string(query_params)
    response = test_app.get(f'/charts/projects_by_credit_totals?{query_string}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('category', ['forest', None])
def test_get_projects_by_category(test_app: TestClient, category: str | None):
    """Test the projects_by_category endpoint."""
    query_params = {'category': category} if category else {}
    query_string = generate_query_string(query_params)
    response = test_app.get(f'/charts/projects_by_category?{query_string}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('category', ['forest', None])
def test_get_credits_by_category(test_app: TestClient, category: str | None):
    """Test the credits_by_category endpoint."""
    query_params = {'category': category} if category else {}
    query_string = generate_query_string(query_params)
    response = test_app.get(f'/charts/credits_by_category?{query_string}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)
