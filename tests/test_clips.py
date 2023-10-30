import pytest


def test_get_clips(test_app):
    response = test_app.get('/clips/')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)


@pytest.mark.parametrize('article_type', ['foo'])
@pytest.mark.parametrize('tags', ['foo'])
def test_get_filtered_clips(test_app, article_type, tags):
    response = test_app.get(f'/clips/?article_type={article_type}&tags={tags}&search=carbon')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)
