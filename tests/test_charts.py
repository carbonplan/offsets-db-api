import pandas as pd
import pytest

from offsets_db_api.routers.charts import filter_valid_projects, projects_by_category


@pytest.fixture
def sample_projects():
    data = pd.DataFrame(
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

    return data


@pytest.mark.parametrize(
    'categories, expected',
    [
        (None, ['ghg-management', 'renewable-energy', 'biodiversity', 'water-management']),
        (['renewable-energy'], ['renewable-energy']),
    ],
)
def test_filter_valid_projects(sample_projects, categories, expected):
    result = filter_valid_projects(sample_projects, categories=categories)
    assert list(result['category'].unique()) == expected


@pytest.mark.parametrize(
    'categories,expected',
    [
        (
            None,
            [
                {'category': 'ghg-management', 'value': 2},
                {'category': 'renewable-energy', 'value': 1},
                {'category': 'biodiversity', 'value': 1},
                {'category': 'water-management', 'value': 1},
            ],
        ),
        (['ghg-management'], [{'category': 'ghg-management', 'value': 2}]),
        (
            ['renewable-energy', 'biodiversity'],
            [
                {'category': 'renewable-energy', 'value': 1},
                {'category': 'biodiversity', 'value': 1},
            ],
        ),
        (['non-existent-category'], []),
        ([], []),
    ],
)
def test_projects_by_category(categories, expected, sample_projects):
    result = projects_by_category(df=sample_projects, categories=categories)
    sorted_result = sorted(result, key=lambda x: x['category'])
    sorted_expected = sorted(expected, key=lambda x: x['category'])
    assert sorted_result == sorted_expected


@pytest.mark.parametrize('freq', ['D', 'M', 'Y', 'W'])
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
@pytest.mark.parametrize('is_compliance', [True, False])
def test_get_projects_by_listing_date(
    test_app,
    freq,
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
    is_compliance,
):
    response = test_app.get(
        f'/charts/projects_by_listing_date?freq={freq}&registry={registry}&country={country}&protocol={protocol}&category={category}&listed_at_from={listed_at_from}&listed_at_to={listed_at_to}&search={search}&retired_min={retired_min}&retired_max={retired_max}&issued_min={issued_min}&issued_max={issued_max}&is_compliance={is_compliance}'
    )
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('freq', ['D', 'M', 'Y', 'W'])
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
@pytest.mark.parametrize('is_compliance', [True, False])
def test_get_credits_by_transaction_date(
    test_app,
    freq,
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
    is_compliance,
):
    response = test_app.get(
        f'/charts/credits_by_transaction_date?freq={freq}&registry={registry}&country={country}&protocol={protocol}&category={category}&listed_at_from={listed_at_from}&listed_at_to={listed_at_to}&search={search}&retired_min={retired_min}&retired_max={retired_max}&issued_min={issued_min}&issued_max={issued_max}&is_compliance={is_compliance}'
    )
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('num_bins', [5, 20, 30])
@pytest.mark.parametrize('transaction_type', ['issuance', 'retirement'])
@pytest.mark.parametrize('vintage', range(2015, 2023))
@pytest.mark.parametrize('transaction_date_from', ['2020-01-01'])
@pytest.mark.parametrize('transaction_date_to', ['2023-01-01', '2024-01-01'])
def test_get_credits_by_transaction_date_by_project(
    test_app,
    num_bins,
    transaction_type,
    vintage,
    transaction_date_from,
    transaction_date_to,
):
    project_id = 'ACR462'

    response = test_app.get(
        f'/charts/credits_by_transaction_date/{project_id}/?num_bins={num_bins}&transaction_type={transaction_type}&vintage={vintage}&transaction_date_from={transaction_date_from}&transaction_date_to={transaction_date_to}'
    )
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


def test_get_credits_by_transaction_date_by_nonexistent_project(test_app):
    project_id = 'ACR999'
    response = test_app.get(f'/charts/credits_by_transaction_date/{project_id}/')
    assert response.status_code == 200
    # check that the response is empty
    assert response.json()['data'] == []


@pytest.mark.parametrize('credit_type', ['issued', 'retired'])
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
@pytest.mark.parametrize('is_compliance', [True, False])
def test_get_projects_by_credit_totals(
    test_app,
    credit_type,
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
    is_compliance,
):
    response = test_app.get(
        f'/charts/projects_by_credit_totals?credit_type={credit_type}&registry={registry}&country={country}&protocol={protocol}&category={category}&listed_at_from={listed_at_from}&listed_at_to={listed_at_to}&search={search}&retired_min={retired_min}&retired_max={retired_max}&issued_min={issued_min}&issued_max={issued_max}&is_compliance={is_compliance}'
    )
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('category', ['forest', None])
def test_get_projects_by_category(test_app, category):
    response = test_app.get(f'/charts/projects_by_category?category={category}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('category', ['forest', None])
def test_get_credits_by_category(test_app, category):
    response = test_app.get(f'/charts/credits_by_category?category={category}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)
