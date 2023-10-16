# from unittest import mock

# import pytest
# from starlette.datastructures import URL, QueryParams

# from carbonplan_offsets_db.query_helpers import _generate_next_page_url, custom_urlencode


# @pytest.mark.parametrize(
#     'input_dict, expected_output',
#     [
#         ({'key': 'value'}, 'key=value'),
#         ({'key1': 'value1', 'key2': 'value2'}, 'key1=value1&key2=value2'),
#         ({'key': ['value1', 'value2']}, 'key=value1&key=value2'),
#         ({'key1': 'value1', 'key2': ['value2', 'value3']}, 'key1=value1&key2=value2&key2=value3'),
#         ({'key with space': 'value/slash'}, 'key%20with%20space=value%2Fslash'),
#     ],
# )
# def test_custom_urlencode(input_dict, expected_output):
#     assert custom_urlencode(input_dict) == expected_output


# @pytest.mark.parametrize(
#     'query_string, current_page, per_page, expected_output',
#     [
#         ('', 1, 10, 'http://testserver?current_page=2&per_page=10'),
#         ('key=value', 1, 10, 'http://testserver?key=value&current_page=2&per_page=10'),
#         (
#             'key1=value1&key2=value2',
#             1,
#             10,
#             'http://testserver?key1=value1&key2=value2&current_page=2&per_page=10',
#         ),
#         (
#             'key=value1&key=value2',
#             1,
#             10,
#             'http://testserver?key=value1&key=value2&current_page=2&per_page=10',
#         ),
#         (
#             'key with space=value/slash',
#             1,
#             10,
#             'http://testserver?key%20with%20space=value%2Fslash&current_page=2&per_page=10',
#         ),
#     ],
# )
# def test_generate_next_page_url(query_string, current_page, per_page, expected_output):
#     with mock.patch('fastapi.Request') as MockRequest:
#         MockRequest.url = URL('http://testserver')
#         MockRequest.query_params = QueryParams(query_string)
#         assert (
#             _generate_next_page_url(
#                 request=MockRequest, current_page=current_page, per_page=per_page
#             )
#             == expected_output
#         )
