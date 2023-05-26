import pytest


def test_get_credits(test_app):
    response = test_app.get('/credits/')
    assert response.status_code == 200
    if response.json():
        # Since the database is pre-populated, we should expect at least one credit
        assert len(response.json()) > 0
        # Verify the structure of a single returned credit
        credit = response.json()[0]
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
def test_get_credits_with_filters(test_app, transaction_type, project_id, vintage):
    response = test_app.get(
        f'/credits/?transaction_type={transaction_type}&project_id={project_id}&vintage={vintage}&sort=-vintage'
    )
    assert response.status_code == 200
    if response.json():
        # Verify that all returned credits match the filters
        for credit in response.json():
            assert credit['transaction_type'] == transaction_type
            assert credit['project_id'] == project_id
            assert credit['vintage'] == vintage
