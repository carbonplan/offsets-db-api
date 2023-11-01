import pytest


@pytest.mark.parametrize('category', ['forest', None])
def test_get_projects_by_category(test_app, category):
    response = test_app.get(f'/stats/projects_by_category?category={category}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)


@pytest.mark.parametrize('category', ['forest', None])
def test_get_credits_by_category(test_app, category):
    response = test_app.get(f'/stats/credits_by_category?category={category}')
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)
