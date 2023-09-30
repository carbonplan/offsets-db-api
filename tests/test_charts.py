import pytest


@pytest.mark.parametrize('freq', ['D', 'M', 'Y', 'W'])
@pytest.mark.parametrize('registry', ['american-carbon-registry', 'climate-action-reserve'])
@pytest.mark.parametrize('country', ['US', 'CA'])
@pytest.mark.parametrize('protocol', [None, 'foo'])
@pytest.mark.parametrize('category', ['other'])
@pytest.mark.parametrize('started_at_from', ['2020-01-01'])
@pytest.mark.parametrize('started_at_to', ['2023-01-01'])
@pytest.mark.parametrize('search', ['foo'])
@pytest.mark.parametrize('retired_min', [0])
@pytest.mark.parametrize('retired_max', [100000])
@pytest.mark.parametrize('issued_min', [0])
@pytest.mark.parametrize('issued_max', [100000])
@pytest.mark.parametrize('is_arb', [True, False])
def test_get_projects_by_registration_date(
    test_app,
    freq,
    registry,
    country,
    protocol,
    category,
    started_at_from,
    started_at_to,
    search,
    retired_min,
    retired_max,
    issued_min,
    issued_max,
    is_arb,
):
    response = test_app.get(
        f'/charts/projects_by_registration_date?freq={freq}&registry={registry}&country={country}&protocol={protocol}&category={category}&started_at_from={started_at_from}&started_at_to={started_at_to}&search={search}&retired_min={retired_min}&retired_max={retired_max}&issued_min={issued_min}&issued_max={issued_max}&is_arb={is_arb}'
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


@pytest.mark.parametrize('freq', ['D', 'M', 'Y', 'W'])
@pytest.mark.parametrize('registry', ['american-carbon-registry', 'climate-action-reserve'])
@pytest.mark.parametrize('country', ['US', 'CA'])
@pytest.mark.parametrize('protocol', [None, 'foo'])
@pytest.mark.parametrize('category', ['other'])
@pytest.mark.parametrize('started_at_from', ['2020-01-01'])
@pytest.mark.parametrize('started_at_to', ['2023-01-01'])
@pytest.mark.parametrize('search', ['foo'])
@pytest.mark.parametrize('retired_min', [0])
@pytest.mark.parametrize('retired_max', [100000])
@pytest.mark.parametrize('issued_min', [0])
@pytest.mark.parametrize('issued_max', [100000])
@pytest.mark.parametrize('is_arb', [True, False])
def test_get_credits_by_transaction_date(
    test_app,
    freq,
    registry,
    country,
    protocol,
    category,
    started_at_from,
    started_at_to,
    search,
    retired_min,
    retired_max,
    issued_min,
    issued_max,
    is_arb,
):
    response = test_app.get(
        f'/charts/credits_by_transaction_date?freq={freq}&registry={registry}&country={country}&protocol={protocol}&category={category}&started_at_from={started_at_from}&started_at_to={started_at_to}&search={search}&retired_min={retired_min}&retired_max={retired_max}&issued_min={issued_min}&issued_max={issued_max}&is_arb={is_arb}'
    )
    assert response.status_code == 200
    data = response.json()
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
    response = test_app.get('/projects')
    assert response.status_code == 200
    data = response.json()['data'][0]
    project_id = data['project_id']

    response = test_app.get(
        f'/charts/credits_by_transaction_date/{project_id}/?num_bins={num_bins}&transaction_type={transaction_type}&vintage={vintage}&transaction_date_from={transaction_date_from}&transaction_date_to={transaction_date_to}'
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
