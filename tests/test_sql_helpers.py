from unittest.mock import Mock

import pytest
from starlette.datastructures import URL, QueryParams

from offsets_db_api.sql_helpers import _generate_next_page_url, custom_urlencode


@pytest.fixture
def mock_request():
    request = Mock()
    request.url = URL('http://testserver')
    return request


@pytest.mark.parametrize(
    'input_dict, expected_output',
    [
        ({'key': 'value'}, 'key=value'),
        ({'key1': 'value1', 'key2': 'value2'}, 'key1=value1&key2=value2'),
        ({'key': ['value1', 'value2']}, 'key=value1&key=value2'),
        ({'key1': 'value1', 'key2': ['value2', 'value3']}, 'key1=value1&key2=value2&key2=value3'),
        ({'key with space': 'value/slash'}, 'key%20with%20space=value%2Fslash'),
    ],
)
def test_custom_urlencode(input_dict: dict[str, str | list[str]], expected_output: str):
    """Test custom_urlencode function with various input dictionaries."""
    assert custom_urlencode(input_dict) == expected_output


@pytest.mark.parametrize(
    'query_string, current_page, per_page, expected_output',
    [
        ('', 1, 10, 'http://testserver?current_page=2&per_page=10'),
        ('key=value', 1, 10, 'http://testserver?key=value&current_page=2&per_page=10'),
        (
            'key1=value1&key2=value2',
            1,
            10,
            'http://testserver?key1=value1&key2=value2&current_page=2&per_page=10',
        ),
        (
            'key=value1&key=value2',
            1,
            10,
            'http://testserver?key=value1&key=value2&current_page=2&per_page=10',
        ),
        (
            'key with space=value/slash',
            1,
            10,
            'http://testserver?key%20with%20space=value%2Fslash&current_page=2&per_page=10',
        ),
    ],
)
def test_generate_next_page_url(
    mock_request: Mock, query_string: str, current_page: int, per_page: int, expected_output: str
):
    """Test _generate_next_page_url function with various query strings and pagination parameters."""
    mock_request.query_params = QueryParams(query_string)

    result = _generate_next_page_url(
        request=mock_request, current_page=current_page, per_page=per_page
    )

    assert result == expected_output


@pytest.mark.parametrize(
    'current_page, per_page',
    [
        (1, 20),
        (2, 15),
        (3, 30),
    ],
)
def test_generate_next_page_url_pagination(mock_request: Mock, current_page: int, per_page: int):
    """Test _generate_next_page_url function focuses on correct pagination parameter handling."""
    mock_request.query_params = QueryParams('')

    result = _generate_next_page_url(
        request=mock_request, current_page=current_page, per_page=per_page
    )

    expected_next_page = current_page + 1
    assert f'current_page={expected_next_page}' in result
    assert f'per_page={per_page}' in result


def test_generate_next_page_url_preserves_existing_params(mock_request: Mock):
    """Test _generate_next_page_url function preserves existing query parameters."""
    mock_request.query_params = QueryParams('existing=param&another=value')

    result = _generate_next_page_url(request=mock_request, current_page=1, per_page=10)

    assert 'existing=param' in result
    assert 'another=value' in result
    assert 'current_page=2' in result
    assert 'per_page=10' in result
