import pytest


def test_get_clips(test_app):
    response = test_app.get('/clips/')
    assert response.status_code == 200
    assert isinstance(response.json()['data'], list)


@pytest.mark.parametrize(
    'source, type, tags, search, project_id',
    [('ZEIT', 'press', 'additionality', 'carbon', 'VCS994')],
)
def test_get_filtered_clips(test_app, source, type, tags, search, project_id):
    response = test_app.get(
        f'/clips/?type={type}&tags={tags}&search={search}&source={source}&project_id={project_id}&sort=-date'
    )
    assert response.status_code == 200
    data = response.json()['data']
    assert isinstance(data, list)

    assert data[0]['type'] == type
    assert tags in data[0]['tags']
    assert search in data[0]['title'].lower()
    assert source in data[0]['source']
    assert project_id in data[0]['project_ids']
