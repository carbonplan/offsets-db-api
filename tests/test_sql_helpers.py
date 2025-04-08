from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Column, Integer, String, select
from sqlalchemy.ext.declarative import declarative_base
from sqlmodel import Session
from starlette.datastructures import URL, QueryParams

from offsets_db_api.models import Credit, Project
from offsets_db_api.schemas import ProjectTypes
from offsets_db_api.sql_helpers import (
    _convert_query_params_to_dict,
    _generate_next_page_url,
    apply_beneficiary_search,
    apply_filters,
    custom_urlencode,
    expand_project_types,
    handle_pagination,
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


@patch('offsets_db_api.sql_helpers.get_project_types')
def test_expand_project_types_none(mock_get_project_types):
    """Test expand_project_types with None input."""
    session = Mock(spec=Session)
    result = expand_project_types(session, None)
    assert result is None
    mock_get_project_types.assert_not_called()


@patch('offsets_db_api.sql_helpers.get_project_types')
def test_expand_project_types_without_other(mock_get_project_types):
    """Test expand_project_types with list not containing 'Other'."""
    session = Mock(spec=Session)
    project_types = ['Type1', 'Type2']
    result = expand_project_types(session, project_types)
    assert result == project_types
    mock_get_project_types.assert_not_called()


@patch('offsets_db_api.sql_helpers.get_project_types')
def test_expand_project_types_with_other(mock_get_project_types):
    """Test expand_project_types with list containing 'Other'."""
    session = Mock(spec=Session)
    mock_get_project_types.return_value = ProjectTypes(
        Top=['Type1', 'Type2'], Other=['Type3', 'Type4']
    )

    project_types = ['Type1', 'Other']
    result = expand_project_types(session, project_types)

    assert 'Other' not in result
    assert 'Type1' in result
    assert 'Type3' in result
    assert 'Type4' in result
    mock_get_project_types.assert_called_once_with(session)


def test_apply_filters_none_values():
    """Test apply_filters with None values."""
    Base = declarative_base()

    class TestModel(Base):
        __tablename__ = 'test_model'
        id = Column(Integer, primary_key=True)
        name = Column(String)

    stmt = select(TestModel)
    result = apply_filters(
        statement=stmt, model=TestModel, attribute='name', values=None, operation='=='
    )

    # Should return the statement unchanged if values is None
    assert result == stmt


def test_apply_filters_unsupported_operation():
    """Test apply_filters with an unsupported operation."""
    Base = declarative_base()

    class TestModel(Base):
        __tablename__ = 'test_model'
        id = Column(Integer, primary_key=True)
        name = Column(String)

    stmt = select(TestModel)

    with pytest.raises(ValueError) as excinfo:
        apply_filters(
            statement=stmt,
            model=TestModel,
            attribute='name',
            values='test',
            operation='invalid_op',
        )

    assert 'Unsupported operation' in str(excinfo.value)


def test_apply_beneficiary_search_empty_search_term():
    """Test apply_beneficiary_search with empty search term."""
    statement = Mock()
    search_term = ''
    search_fields = ['name', 'description']

    result = apply_beneficiary_search(
        statement=statement,
        search_term=search_term,
        search_fields=search_fields,
        credit_model=Credit,
        project_model=Project,
    )

    # Should return statement unchanged if search_term is empty
    assert result == statement


@patch('offsets_db_api.sql_helpers._generate_next_page_url')
def test_handle_pagination_no_next_page(mock_generate_next_page_url):
    """Test handle_pagination when current_page equals total_pages."""
    # Setup mocks
    statement = Mock()
    primary_key = 'id'
    current_page = 2
    per_page = 10
    request = Mock()
    session = Mock()

    # Mock the session.exec().one() to return a count
    session.exec.return_value.one.return_value = 15  # 15 items total

    # Mock the session.exec().all() to return results
    session.exec.return_value.all.return_value = ['result1', 'result2']

    total_entries, current_page_result, total_pages, next_page, results = handle_pagination(
        statement=statement,
        primary_key=primary_key,
        current_page=current_page,
        per_page=per_page,
        request=request,
        session=session,
    )

    assert total_entries == 15
    assert current_page_result == 2
    assert total_pages == 2  # 15 items with 10 per page = 2 pages
    assert next_page is None  # No next page because current_page == total_pages
    assert results == ['result1', 'result2']
    mock_generate_next_page_url.assert_not_called()  # URL generation should not be called


@pytest.mark.parametrize(
    'input_dict, expected_output',
    [
        ({'key+with+plus': 'value+with+plus'}, 'key%2Bwith%2Bplus=value%2Bwith%2Bplus'),
        (
            {'key&with&ampersand': 'value&with&ampersand'},
            'key%26with%26ampersand=value%26with%26ampersand',
        ),
        (
            {'key?with?question': 'value?with?question'},
            'key%3Fwith%3Fquestion=value%3Fwith%3Fquestion',
        ),
        ({'key=with=equals': 'value=with=equals'}, 'key%3Dwith%3Dequals=value%3Dwith%3Dequals'),
        ({'key#with#hash': 'value#with#hash'}, 'key%23with%23hash=value%23with%23hash'),
        (
            {'key with space': ['value with space', 'another value']},
            'key%20with%20space=value%20with%20space&key%20with%20space=another%20value',
        ),
    ],
)
def test_custom_urlencode_special_characters(input_dict, expected_output):
    """Test custom_urlencode function with special characters."""
    assert custom_urlencode(input_dict) == expected_output
