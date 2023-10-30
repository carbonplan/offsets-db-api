import pytest


def test_get_clips(test_app):
    response = test_app.get('/clips/')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)


@pytest.mark.parametrize('article_type', ['foo'])
def test_get_filtered_clips(test_app, article_type):
    response = test_app.get(f'/clips/?article_type={article_type}')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)
