import datetime
from unittest.mock import patch

import pytest

from offsets_db_api.models import Credit, Project


@pytest.fixture
def mock_credits():
    """Mock credit data"""
    return [
        Credit(
            id=1,
            project_id='VCS123',
            vintage=2020,
            quantity=1000,
            status='issued',
            unit_type='VCU',
            serial_number='VCS-123-456',
            registry='verra',
            issued_at=datetime.date(2020, 5, 15),
            retirement_beneficiary='Company A',
            retirement_reason='Carbon neutrality',
            retirement_details='Annual offset program',
            retired_at=datetime.date(2021, 1, 10),
        ),
        Credit(
            id=2,
            project_id='GS456',
            vintage=2019,
            quantity=500,
            status='retired',
            unit_type='GS-CER',
            serial_number='GS-456-789',
            registry='gold-standard',
            issued_at=datetime.date(2019, 8, 20),
            retirement_beneficiary='Company B',
            retirement_reason='Voluntary offset',
            retirement_details='Product line offsetting',
            retired_at=datetime.date(2020, 3, 15),
        ),
    ]


@pytest.fixture
def mock_projects():
    """Mock project data that corresponds to the mock credits"""
    return [
        Project(
            project_id='VCS123',
            name='Test Forest Project',
            registry='verra',
            category='forest',
            type='forest/afforestation',
        ),
        Project(
            project_id='GS456',
            name='Test Renewable Project',
            registry='gold-standard',
            category='renewable',
            type='renewable-energy/solar',
        ),
    ]


class TestGetCreditsEndpoint:
    @patch('offsets_db_api.routers.credits.handle_pagination')
    @patch('offsets_db_api.routers.credits.apply_sorting')
    @patch('offsets_db_api.routers.credits.apply_filters')
    @patch('offsets_db_api.routers.credits.build_filters')
    @patch('offsets_db_api.routers.credits.select')
    def test_get_credits_basic(
        self,
        mock_select,
        mock_build_filters,
        mock_apply_filters,
        mock_apply_sorting,
        mock_handle_pagination,
        test_app,
        mock_credits,
        mock_projects,
    ):
        """Test basic credit retrieval with no filters"""
        # Create mock results (tuples of credit and project)
        mock_results = list(zip(mock_credits, mock_projects))

        # Mock the response from handle_pagination
        mock_handle_pagination.return_value = (
            len(mock_results),  # total_entries
            1,  # current_page
            1,  # total_pages
            None,  # next_page
            mock_results,  # results - list of (credit, project) tuples
        )

        # Mock the build_filters to return an empty list (no filters)
        mock_build_filters.return_value = []

        # Call the endpoint
        response = test_app.get('/credits/')

        # Verify mocks were called
        mock_select.assert_called_once()
        mock_build_filters.assert_called_once()
        mock_apply_sorting.assert_called_once()
        mock_handle_pagination.assert_called_once()

        # Check response structure
        assert response.status_code == 200
        data = response.json()
        assert 'pagination' in data
        assert 'data' in data

    @patch('offsets_db_api.routers.credits.apply_beneficiary_search')
    @patch('offsets_db_api.routers.credits.handle_pagination')
    @patch('offsets_db_api.routers.credits.apply_sorting')
    @patch('offsets_db_api.routers.credits.apply_filters')
    @patch('offsets_db_api.routers.credits.build_filters')
    @patch('offsets_db_api.routers.credits.select')
    def test_get_credits_with_beneficiary_search(
        self,
        mock_select,
        mock_build_filters,
        mock_apply_filters,
        mock_apply_sorting,
        mock_handle_pagination,
        mock_apply_beneficiary_search,
        test_app,
        mock_credits,
        mock_projects,
    ):
        """Test credits retrieval with beneficiary search parameter"""
        # Create mock results with only the first credit-project pair
        mock_results = [(mock_credits[0], mock_projects[0])]

        # Mock the response from handle_pagination
        mock_handle_pagination.return_value = (
            1,  # total_entries
            1,  # current_page
            1,  # total_pages
            None,  # next_page
            mock_results,  # results - only the first credit-project pair
        )

        # Mock build_filters to return an empty list
        mock_build_filters.return_value = []

        # Configure beneficiary_search to be triggered
        mock_apply_beneficiary_search.return_value = 'MODIFIED_STATEMENT'

        # Call the endpoint
        response = test_app.get('/credits/?beneficiary_search=Company+A')

        # Verify response
        assert response.status_code == 200

        # Verify mocks were called
        mock_apply_beneficiary_search.assert_called_once()
        mock_select.assert_called_once()
        mock_handle_pagination.assert_called_once()

    @patch('offsets_db_api.routers.credits.handle_pagination')
    @patch('offsets_db_api.routers.credits.apply_sorting')
    @patch('offsets_db_api.routers.credits.apply_filters')
    @patch('offsets_db_api.routers.credits.build_filters')
    @patch('offsets_db_api.routers.credits.select')
    def test_get_credits_with_filters(
        self,
        mock_select,
        mock_build_filters,
        mock_apply_filters,
        mock_apply_sorting,
        mock_handle_pagination,
        test_app,
        mock_credits,
        mock_projects,
    ):
        """Test credits retrieval with filters applied"""
        # Create mock results with only the first credit-project pair
        mock_results = [(mock_credits[0], mock_projects[0])]

        # Mock the response from handle_pagination
        mock_handle_pagination.return_value = (
            1,  # total_entries
            1,  # current_page
            1,  # total_pages
            None,  # next_page
            mock_results,  # results - only the first credit
        )

        # Set build_filters to return a list of filter criteria
        mock_build_filters.return_value = [
            ('registry', ['verra'], '==', Credit),
            ('vintage', [2020], '==', Credit),
            ('status', ['issued'], '==', Credit),
        ]

        # Call the endpoint
        response = test_app.get('/credits/?registry=verra&vintage=2020&status=issued')

        # Verify response
        assert response.status_code == 200

        # Verify mocks were called
        mock_build_filters.assert_called_once()
        mock_apply_filters.assert_called()  # Should be called multiple times
        mock_handle_pagination.assert_called_once()

    @patch('offsets_db_api.routers.credits.handle_pagination')
    @patch('offsets_db_api.routers.credits.apply_sorting')
    @patch('offsets_db_api.routers.credits.apply_filters')
    @patch('offsets_db_api.routers.credits.build_filters')
    @patch('offsets_db_api.routers.credits.select')
    def test_get_credits_with_project_id_filter(
        self,
        mock_select,
        mock_build_filters,
        mock_apply_filters,
        mock_apply_sorting,
        mock_handle_pagination,
        test_app,
        mock_credits,
        mock_projects,
    ):
        """Test credits retrieval with project_id filter"""
        # Filter for VCS123 project
        filtered_credits = [mock_credits[0]]
        filtered_projects = [mock_projects[0]]
        mock_results = list(zip(filtered_credits, filtered_projects))

        # Mock the response from handle_pagination
        mock_handle_pagination.return_value = (
            len(filtered_credits),  # total_entries
            1,  # current_page
            1,  # total_pages
            None,  # next_page
            mock_results,  # results - only credits for VCS123
        )

        # Build filters will be called but we're checking for project_id separately
        mock_build_filters.return_value = []

        # Call the endpoint
        response = test_app.get('/credits/?project_id=VCS123')

        # Verify response
        assert response.status_code == 200

        # Verify mocks were called
        mock_build_filters.assert_called_once()
        mock_apply_filters.assert_called()
        mock_handle_pagination.assert_called_once()

    @patch('offsets_db_api.routers.credits.handle_pagination')
    @patch('offsets_db_api.routers.credits.apply_sorting')
    @patch('offsets_db_api.routers.credits.apply_filters')
    @patch('offsets_db_api.routers.credits.build_filters')
    @patch('offsets_db_api.routers.credits.select')
    def test_get_credits_with_custom_sorting(
        self,
        mock_select,
        mock_build_filters,
        mock_apply_filters,
        mock_apply_sorting,
        mock_handle_pagination,
        test_app,
        mock_credits,
        mock_projects,
    ):
        """Test credits retrieval with custom sorting"""
        # Create mock results (sorted by quantity descending)
        mock_results = [(mock_credits[0], mock_projects[0]), (mock_credits[1], mock_projects[1])]

        # Mock the response from handle_pagination
        mock_handle_pagination.return_value = (
            len(mock_results),
            1,
            1,
            None,
            mock_results,
        )

        # Mock build_filters to return an empty list
        mock_build_filters.return_value = []

        # Call the endpoint with custom sort
        response = test_app.get('/credits/?sort=-quantity')

        # Verify response
        assert response.status_code == 200

        # Verify mocks were called
        mock_apply_sorting.assert_called_once()
        # Check sort arguments
        args, kwargs = mock_apply_sorting.call_args
        assert 'sort' in kwargs

    @patch('offsets_db_api.routers.credits.handle_pagination')
    @patch('offsets_db_api.routers.credits.apply_sorting')
    @patch('offsets_db_api.routers.credits.apply_filters')
    @patch('offsets_db_api.routers.credits.build_filters')
    @patch('offsets_db_api.routers.credits.select')
    def test_get_credits_with_pagination(
        self,
        mock_select,
        mock_build_filters,
        mock_apply_filters,
        mock_apply_sorting,
        mock_handle_pagination,
        test_app,
        mock_credits,
        mock_projects,
    ):
        """Test credits retrieval with pagination parameters"""
        # Mock the response for pagination
        mock_handle_pagination.return_value = (
            100,  # total_entries (pretend there are 100 credits)
            2,  # current_page
            5,  # total_pages
            'http://testserver/credits/?page=3&per_page=20',  # next_page
            [(mock_credits[0], mock_projects[0])],  # results for page 2
        )

        # Mock build_filters to return an empty list
        mock_build_filters.return_value = []

        # Call endpoint with pagination parameters
        response = test_app.get('/credits/?current_page=2&per_page=20')

        # Verify response
        assert response.status_code == 200

        # Verify mocks were called
        mock_handle_pagination.assert_called_once()
        # Check pagination arguments
        args, kwargs = mock_handle_pagination.call_args
        assert kwargs.get('current_page') == 2
        assert kwargs.get('per_page') == 20
