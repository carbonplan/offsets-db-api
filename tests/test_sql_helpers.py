from unittest.mock import Mock

import pytest
from starlette.datastructures import URL, QueryParams

from offsets_db_api.sql_helpers import (
    _convert_query_params_to_dict,
    _generate_next_page_url,
    custom_urlencode,
)


@pytest.fixture
def mock_request():
    """Create a mock request object with the necessary attributes."""
    mock = Mock()
    mock.url = URL('http://testserver')
    mock.query_params = QueryParams('')
    return mock


@pytest.mark.parametrize(
    'input_dict, expected_output',
    [
        ({}, ''),  # Empty dict
        ({'key': ''}, 'key='),  # Empty value
        ({'': 'value'}, '=value'),  # Empty key
        ({'key': None}, 'key=None'),  # None value
        ({'key': 123}, 'key=123'),  # Numeric value
        ({'key': True}, 'key=True'),  # Boolean value
        ({'key': ['', None, 123, True]}, 'key=&key=None&key=123&key=True'),  # Mixed values in list
    ],
)
def test_custom_urlencode_edge_cases(input_dict, expected_output):
    """Test custom_urlencode function with edge cases."""
    assert custom_urlencode(input_dict) == expected_output


@pytest.mark.parametrize(
    'query_string, expected_dict',
    [
        ('', {}),  # Empty query string
        ('key=value', {'key': 'value'}),  # Single key-value pair
        (
            'key1=value1&key2=value2',
            {'key1': 'value1', 'key2': 'value2'},
        ),  # Multiple key-value pairs
        ('key=value1&key=value2', {'key': ['value1', 'value2']}),  # Repeated keys
        (
            'key1=value1&key1=value2&key2=value3',
            {'key1': ['value1', 'value2'], 'key2': 'value3'},
        ),  # Mixed repeated and single keys
        ('key=', {'key': ''}),  # Empty value
        ('key', {'key': ''}),  # Key only
    ],
)
def test_convert_query_params_to_dict(query_string, expected_dict, mock_request):
    """Test _convert_query_params_to_dict function with various query strings."""
    mock_request.query_params = QueryParams(query_string)

    result = _convert_query_params_to_dict(mock_request)

    assert result == expected_dict


@pytest.mark.parametrize(
    'url_parts, query_string, current_page, per_page, expected_output',
    [
        # Test with different URL schemes, paths and hosts
        (
            ('https', 'example.com', '/api/v1/resources'),
            '',
            1,
            10,
            'https://example.com/api/v1/resources?current_page=2&per_page=10',
        ),
        (
            ('http', 'api.example.org', '/search'),
            'q=test',
            1,
            10,
            'http://api.example.org/search?q=test&current_page=2&per_page=10',
        ),
        (
            ('https', 'subdomain.example.com:8443', '/path/to/resource'),
            'filter=active',
            1,
            10,
            'https://subdomain.example.com:8443/path/to/resource?filter=active&current_page=2&per_page=10',
        ),
    ],
)
def test_generate_next_page_url_with_different_urls(
    url_parts, query_string, current_page, per_page, expected_output, mock_request
):
    """Test _generate_next_page_url with different URL schemes, hosts, and paths."""
    scheme, netloc, path = url_parts
    mock_request.url = URL(f'{scheme}://{netloc}{path}')
    mock_request.query_params = QueryParams(query_string)

    result = _generate_next_page_url(
        request=mock_request, current_page=current_page, per_page=per_page
    )

    assert result == expected_output


def test_generate_next_page_url_overrides_existing_pagination_params(mock_request):
    """Test that _generate_next_page_url correctly overrides existing pagination parameters."""
    mock_request.url = URL('http://testserver')
    mock_request.query_params = QueryParams('current_page=3&per_page=5&filter=active')

    result = _generate_next_page_url(request=mock_request, current_page=3, per_page=5)

    assert 'current_page=4' in result  # Should increment to next page
    assert 'per_page=5' in result  # Should preserve the per_page value
    assert 'filter=active' in result  # Should preserve other parameters


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
