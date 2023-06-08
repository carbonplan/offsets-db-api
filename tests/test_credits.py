import pytest


def test_get_credits(test_app):
    response = test_app.get('/credits/?per_page=1&current_page=1')
    assert response.status_code == 200

    if response.json()['data']:
        # Since the database is pre-populated, we should expect at least one credit
        data = response.json()['data']
        assert len(data) == 1
        # Verify the structure of a single returned credit
        credit = data[0]
        assert 'id' in credit
        assert 'project_id' in credit
        assert 'quantity' in credit
        assert 'vintage' in credit
        assert 'transaction_date' in credit
        assert 'transaction_type' in credit
        assert 'details_url' in credit


def test_get_credits_with_non_existent_route(test_app):
    response = test_app.get('/non_existent_route/')
    assert response.status_code == 404


def test_get_credits_with_wrong_http_verb(test_app):
    response = test_app.post('/credits/')
    assert response.status_code == 405


@pytest.mark.parametrize('transaction_type', ['issuance', 'retirement'])
@pytest.mark.parametrize('project_id', ['ACR0001', 'ACR0002'])
@pytest.mark.parametrize('vintage', [2010, 2011])
@pytest.mark.parametrize('is_arb', [True, False])
def test_get_credits_with_filters(test_app, transaction_type, project_id, vintage, is_arb):
    response = test_app.get(
        f'/credits/?transaction_type={transaction_type}&project_id={project_id}&vintage={vintage}&sort=-vintage&is_arb={is_arb}'
    )
    assert response.status_code == 200
    # Verify that all returned credits match the filters
    for credit in response.json()['data']:
        assert credit['transaction_type'] == transaction_type
        assert credit['project_id'] == project_id
        assert credit['vintage'] == vintage
