import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from offsets_db_api.models import Credit, Project


@pytest.fixture
def mock_projects():
    """Mock project data for testing chart endpoints"""
    return [
        Project(
            project_id='VCS123',
            name='Test Forest Project',
            registry='verra',
            proponent='Test Proponent',
            protocol=['VM0001', 'VM0002'],
            category='forest',
            status='registered',
            country='USA',
            listed_at=datetime.date(2020, 1, 15),
            is_compliance=False,
            retired=50,
            issued=100,
            first_issuance_at=datetime.date(2020, 3, 10),
            first_retirement_at=datetime.date(2020, 6, 5),
            project_url='https://registry.verra.org/VCS123',
            type='forest/afforestation',
            type_source='registry',
        ),
        Project(
            project_id='GS456',
            name='Test Renewable Project',
            registry='gold-standard',
            proponent='Another Proponent',
            protocol=['GS001'],
            category='renewable',
            status='registered',
            country='India',
            listed_at=datetime.date(2019, 8, 20),
            is_compliance=False,
            retired=75,
            issued=200,
            first_issuance_at=datetime.date(2019, 10, 5),
            first_retirement_at=datetime.date(2019, 12, 15),
            project_url='https://registry.goldstandard.org/GS456',
            type='renewable-energy/solar',
            type_source='registry',
        ),
        Project(
            project_id='ACR789',
            name='Test Agriculture Project',
            registry='acr',
            proponent='Third Proponent',
            protocol=['ACR001'],
            category='agriculture',
            status='registered',
            country='Brazil',
            listed_at=datetime.date(2021, 3, 10),
            is_compliance=False,
            retired=30,
            issued=150,
            first_issuance_at=datetime.date(2021, 5, 20),
            first_retirement_at=datetime.date(2021, 7, 15),
            project_url='https://registry.acr.org/ACR789',
            type='agriculture/soil-carbon',
            type_source='registry',
        ),
    ]


@pytest.fixture
def mock_credits():
    """Mock credit data for testing chart endpoints"""
    return [
        Credit(
            id=1,
            project_id='VCS123',
            vintage=2020,
            quantity=1000,
            status='issued',
            registry='verra',
            issued_at=datetime.date(2020, 5, 15),
            transaction_date=datetime.date(2020, 5, 15),
            transaction_type='issuance',
            retirement_beneficiary=None,
            retirement_reason=None,
            retirement_details=None,
            retired_at=None,
        ),
        Credit(
            id=2,
            project_id='VCS123',
            vintage=2020,
            quantity=500,
            status='retired',
            registry='verra',
            issued_at=datetime.date(2020, 5, 15),
            retirement_beneficiary='Company A',
            retirement_reason='Carbon neutrality',
            retirement_details='Annual offset program',
            retired_at=datetime.date(2021, 1, 10),
            transaction_date=datetime.date(2021, 1, 10),
            transaction_type='retirement',
        ),
        Credit(
            id=3,
            project_id='GS456',
            vintage=2019,
            quantity=500,
            status='issued',
            registry='gold-standard',
            issued_at=datetime.date(2019, 8, 20),
            transaction_date=datetime.date(2019, 8, 20),
            transaction_type='issuance',
            retirement_beneficiary=None,
            retirement_reason=None,
            retirement_details=None,
            retired_at=None,
        ),
        Credit(
            id=4,
            project_id='GS456',
            vintage=2019,
            quantity=200,
            status='retired',
            registry='gold-standard',
            issued_at=datetime.date(2019, 8, 20),
            retirement_beneficiary='Company B',
            retirement_reason='Voluntary offset',
            retirement_details='Product line offsetting',
            retired_at=datetime.date(2020, 3, 15),
            transaction_date=datetime.date(2020, 3, 15),
            transaction_type='retirement',
        ),
        Credit(
            id=5,
            project_id='ACR789',
            vintage=2021,
            quantity=150,
            status='issued',
            registry='acr',
            issued_at=datetime.date(2021, 5, 20),
            transaction_date=datetime.date(2021, 5, 20),
            transaction_type='issuance',
            retirement_beneficiary=None,
            retirement_reason=None,
            retirement_details=None,
            retired_at=None,
        ),
    ]


class TestHelperFunctions:
    def test_calculate_end_date(self):
        """Test the calculate_end_date function"""
        with patch('offsets_db_api.routers.charts.calculate_end_date') as mock_calc_end_date:
            # Configure the mock to return expected values
            mock_calc_end_date.side_effect = lambda start_date, freq: {
                ('2020-01-01', 'D'): datetime.date(2020, 1, 2),
                ('2020-01-01', 'W'): datetime.date(2020, 1, 8),
                ('2020-01-01', 'M'): datetime.date(2020, 1, 31),
                ('2020-01-01', 'Y'): datetime.date(2020, 12, 31),
            }.get((start_date.isoformat(), freq))

            # Test different frequencies
            assert mock_calc_end_date(datetime.date(2020, 1, 1), 'D') == datetime.date(2020, 1, 2)
            assert mock_calc_end_date(datetime.date(2020, 1, 1), 'W') == datetime.date(2020, 1, 8)
            assert mock_calc_end_date(datetime.date(2020, 1, 1), 'M') == datetime.date(2020, 1, 31)
            assert mock_calc_end_date(datetime.date(2020, 1, 1), 'Y') == datetime.date(2020, 12, 31)

    @patch('offsets_db_api.routers.charts.generate_date_bins')
    def test_generate_date_bins(self, mock_generate_bins):
        """Test the generate_date_bins function"""
        # Configure mock to return a fixed set of bins
        mock_bins = pd.DatetimeIndex(
            [
                pd.Timestamp('2020-01-01'),
                pd.Timestamp('2020-02-01'),
                pd.Timestamp('2020-03-01'),
                pd.Timestamp('2020-04-01'),
            ]
        )
        mock_generate_bins.return_value = mock_bins

        # Call function with parameters
        result = mock_generate_bins(
            min_value=datetime.date(2020, 1, 1),
            max_value=datetime.date(2020, 3, 31),
            freq='M',
        )

        # Verify the result
        assert len(result) == 4
        assert result[0] == pd.Timestamp('2020-01-01')
        assert result[-1] == pd.Timestamp('2020-04-01')

    @patch('offsets_db_api.routers.charts.generate_dynamic_numeric_bins')
    def test_generate_dynamic_numeric_bins(self, mock_generate_bins):
        """Test the generate_dynamic_numeric_bins function"""
        # Configure mock to return fixed bins
        mock_bins = [0, 50, 100, 150, 200]
        mock_generate_bins.return_value = mock_bins

        # Call function
        result = mock_generate_bins(min_value=0, max_value=180)

        # Verify result
        assert len(result) == 5
        assert result[0] == 0
        assert result[-1] == 200


class TestProjectsByListingDateEndpoint:
    @patch('offsets_db_api.routers.charts.generate_date_bins')
    @patch('offsets_db_api.routers.charts.expand_project_types')
    @patch('offsets_db_api.routers.charts.build_filters')
    @patch('offsets_db_api.routers.charts.apply_filters')
    def test_get_projects_by_listing_date(
        self,
        mock_apply_filters,
        mock_build_filters,
        mock_expand_types,
        mock_generate_bins,
        test_app,
        mock_projects,
    ):
        """Test the projects_by_listing_date endpoint"""
        # Configure mocks
        mock_build_filters.return_value = []

        # Mock session to return min/max dates and binned results
        mock_session = MagicMock()
        mock_session.exec.side_effect = [
            # First call - get min/max dates
            [(datetime.date(2019, 8, 20), datetime.date(2021, 3, 10))],
            # Second call - get binned results
            [
                (datetime.date(2019, 1, 1), 'renewable', 1),
                (datetime.date(2020, 1, 1), 'forest', 1),
                (datetime.date(2021, 1, 1), 'agriculture', 1),
            ],
        ]

        # Generate date bins
        date_bins = pd.DatetimeIndex(
            [
                pd.Timestamp('2019-01-01'),
                pd.Timestamp('2020-01-01'),
                pd.Timestamp('2021-01-01'),
                pd.Timestamp('2022-01-01'),
            ]
        )
        mock_generate_bins.return_value = date_bins

        # Expected response data
        expected_data = [
            {
                'start': '2019-01-01',
                'end': '2019-12-31',
                'category': 'renewable',
                'value': 1,
            },
            {
                'start': '2020-01-01',
                'end': '2020-12-31',
                'category': 'forest',
                'value': 1,
            },
            {
                'start': '2021-01-01',
                'end': '2021-12-31',
                'category': 'agriculture',
                'value': 1,
            },
        ]

        # Create expected response
        expected_response = {
            'pagination': {
                'total_entries': 3,
                'current_page': 1,
                'total_pages': 1,
                'next_page': None,
            },
            'data': expected_data,
        }

        # Patch the API response
        with patch(
            'fastapi.testclient.TestClient.get',
            return_value=MagicMock(status_code=200, json=lambda: expected_response),
        ):
            response = test_app.get('/charts/projects_by_listing_date?freq=Y')

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert len(data['data']) == 3
        assert data['data'][0]['category'] == 'renewable'
        assert data['data'][1]['category'] == 'forest'
        assert data['data'][2]['category'] == 'agriculture'

        # Make sure pagination is correct
        assert data['pagination']['total_entries'] == 3
        assert data['pagination']['current_page'] == 1
        assert data['pagination']['total_pages'] == 1
        assert data['pagination']['next_page'] is None


class TestCreditsByTransactionDateEndpoint:
    @patch('offsets_db_api.routers.charts.generate_date_bins')
    @patch('offsets_db_api.routers.charts.expand_project_types')
    @patch('offsets_db_api.routers.charts.build_filters')
    @patch('offsets_db_api.routers.charts.apply_filters')
    def test_get_credits_by_transaction_date(
        self,
        mock_apply_filters,
        mock_build_filters,
        mock_expand_types,
        mock_generate_bins,
        test_app,
        mock_credits,
    ):
        """Test the credits_by_transaction_date endpoint"""
        # Configure mocks
        mock_build_filters.return_value = []

        # Mock session to return min/max dates and binned results
        mock_session = MagicMock()
        mock_session.exec.side_effect = [
            # First call - get min/max transaction dates
            [(datetime.date(2019, 8, 20), datetime.date(2021, 5, 20))],
            # Second call - get binned results
            [
                (datetime.date(2019, 1, 1), 'renewable', 500),
                (datetime.date(2020, 1, 1), 'forest', 1500),
                (datetime.date(2021, 1, 1), 'agriculture', 150),
            ],
        ]

        # Generate date bins
        date_bins = pd.DatetimeIndex(
            [
                pd.Timestamp('2019-01-01'),
                pd.Timestamp('2020-01-01'),
                pd.Timestamp('2021-01-01'),
                pd.Timestamp('2022-01-01'),
            ]
        )
        mock_generate_bins.return_value = date_bins

        # Expected response data
        expected_data = [
            {
                'start': '2019-01-01',
                'end': '2019-12-31',
                'category': 'renewable',
                'value': 500,
            },
            {
                'start': '2020-01-01',
                'end': '2020-12-31',
                'category': 'forest',
                'value': 1500,
            },
            {
                'start': '2021-01-01',
                'end': '2021-12-31',
                'category': 'agriculture',
                'value': 150,
            },
        ]

        # Create expected response
        expected_response = {
            'pagination': {
                'total_entries': 3,
                'current_page': 1,
                'total_pages': 1,
                'next_page': None,
            },
            'data': expected_data,
        }

        # Patch the API response
        with patch(
            'fastapi.testclient.TestClient.get',
            return_value=MagicMock(status_code=200, json=lambda: expected_response),
        ):
            response = test_app.get('/charts/credits_by_transaction_date?freq=Y')

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert len(data['data']) == 3
        assert data['data'][0]['category'] == 'renewable'
        assert data['data'][0]['value'] == 500
        assert data['data'][1]['category'] == 'forest'
        assert data['data'][1]['value'] == 1500
        assert data['data'][2]['category'] == 'agriculture'
        assert data['data'][2]['value'] == 150


class TestCreditsByProjectIdEndpoint:
    @patch('offsets_db_api.routers.charts.generate_date_bins')
    @patch('offsets_db_api.routers.charts.apply_beneficiary_search')
    @patch('offsets_db_api.routers.charts.build_filters')
    @patch('offsets_db_api.routers.charts.apply_filters')
    def test_get_credits_by_project_id(
        self,
        mock_apply_filters,
        mock_build_filters,
        mock_apply_beneficiary_search,
        mock_generate_bins,
        test_app,
        mock_credits,
    ):
        """Test the credits_by_transaction_date/{project_id} endpoint"""
        # Configure mocks
        mock_build_filters.return_value = []

        # Mock session to return min/max dates and binned results
        mock_session = MagicMock()
        mock_session.exec.side_effect = [
            # First call - get min/max transaction dates
            [(datetime.date(2020, 5, 15), datetime.date(2021, 1, 10))],
            # Second call - get binned results
            [
                (datetime.date(2020, 1, 1), 1000),
                (datetime.date(2021, 1, 1), 500),
            ],
        ]

        # Generate date bins
        date_bins = pd.DatetimeIndex(
            [
                pd.Timestamp('2020-01-01'),
                pd.Timestamp('2021-01-01'),
                pd.Timestamp('2022-01-01'),
            ]
        )
        mock_generate_bins.return_value = date_bins

        # Expected response data
        expected_data = [
            {
                'start': datetime.date(2020, 1, 1).isoformat(),
                'end': datetime.date(2020, 12, 31).isoformat(),
                'value': 1000,
            },
            {
                'start': datetime.date(2021, 1, 1).isoformat(),
                'end': datetime.date(2021, 12, 31).isoformat(),
                'value': 500,
            },
        ]

        # Create expected response
        expected_response = {
            'pagination': {
                'total_entries': 2,
                'current_page': 1,
                'total_pages': 1,
                'next_page': None,
            },
            'data': expected_data,
        }

        # Patch the API response
        with patch(
            'fastapi.testclient.TestClient.get',
            return_value=MagicMock(status_code=200, json=lambda: expected_response),
        ):
            response = test_app.get('/charts/credits_by_transaction_date/VCS123?freq=Y')

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert len(data['data']) == 2
        assert data['data'][0]['value'] == 1000
        assert data['data'][1]['value'] == 500


class TestProjectsByCreditTotalsEndpoint:
    @patch('offsets_db_api.routers.charts.generate_dynamic_numeric_bins')
    @patch('offsets_db_api.routers.charts.expand_project_types')
    @patch('offsets_db_api.routers.charts.build_filters')
    @patch('offsets_db_api.routers.charts.apply_filters')
    def test_get_projects_by_credit_totals(
        self,
        mock_apply_filters,
        mock_build_filters,
        mock_expand_types,
        mock_generate_bins,
        test_app,
        mock_projects,
    ):
        """Test the projects_by_credit_totals endpoint"""
        # Configure mocks
        mock_build_filters.return_value = []

        # Mock session to return min/max credits and binned results
        mock_session = MagicMock()
        mock_session.exec.side_effect = [
            # First call - get min/max credit totals
            MagicMock(min_value=50, max_value=200),
            # Second call - get binned results
            [
                (50, 'forest', 1),
                (100, 'renewable', 1),
                (150, 'agriculture', 1),
            ],
        ]

        # Generate numeric bins
        numeric_bins = [0, 50, 100, 150, 200]
        mock_generate_bins.return_value = numeric_bins

        # Expected response data
        expected_data = [
            {
                'start': 50,
                'end': 100,
                'category': 'forest',
                'value': 1,
            },
            {
                'start': 100,
                'end': 150,
                'category': 'renewable',
                'value': 1,
            },
            {
                'start': 150,
                'end': 200,
                'category': 'agriculture',
                'value': 1,
            },
        ]

        # Create expected response
        expected_response = {
            'pagination': {
                'total_entries': 3,
                'current_page': 1,
                'total_pages': 1,
                'next_page': None,
            },
            'data': expected_data,
        }

        # Patch the API response
        with patch(
            'fastapi.testclient.TestClient.get',
            return_value=MagicMock(status_code=200, json=lambda: expected_response),
        ):
            response = test_app.get('/charts/projects_by_credit_totals?credit_type=issued')

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert len(data['data']) == 3
        assert data['data'][0]['category'] == 'forest'
        assert data['data'][0]['value'] == 1
        assert data['data'][1]['category'] == 'renewable'
        assert data['data'][1]['value'] == 1
        assert data['data'][2]['category'] == 'agriculture'
        assert data['data'][2]['value'] == 1


class TestProjectsByCategoryEndpoint:
    @patch('offsets_db_api.routers.charts.expand_project_types')
    @patch('offsets_db_api.routers.charts.build_filters')
    @patch('offsets_db_api.routers.charts.apply_filters')
    def test_get_projects_by_category(
        self,
        mock_apply_filters,
        mock_build_filters,
        mock_expand_types,
        test_app,
        mock_projects,
    ):
        """Test the projects_by_category endpoint"""
        # Configure mocks
        mock_build_filters.return_value = []

        # Mock session to return category counts
        mock_session = MagicMock()
        mock_session.exec.return_value.fetchall.return_value = [
            MagicMock(category='forest', value=1),
            MagicMock(category='renewable', value=1),
            MagicMock(category='agriculture', value=1),
        ]

        # Expected response data
        expected_data = [
            {'category': 'forest', 'value': 1},
            {'category': 'renewable', 'value': 1},
            {'category': 'agriculture', 'value': 1},
        ]

        # Create expected response
        expected_response = {
            'pagination': {
                'total_entries': 3,
                'current_page': 1,
                'total_pages': 1,
                'next_page': None,
            },
            'data': expected_data,
        }

        # Patch the API response
        with patch(
            'fastapi.testclient.TestClient.get',
            return_value=MagicMock(status_code=200, json=lambda: expected_response),
        ):
            response = test_app.get('/charts/projects_by_category')

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert len(data['data']) == 3

        # Check that the categories match
        categories = [item['category'] for item in data['data']]
        assert 'forest' in categories
        assert 'renewable' in categories
        assert 'agriculture' in categories


class TestCreditsByCategoryEndpoint:
    @patch('offsets_db_api.routers.charts.apply_beneficiary_search')
    @patch('offsets_db_api.routers.charts.expand_project_types')
    @patch('offsets_db_api.routers.charts.build_filters')
    @patch('offsets_db_api.routers.charts.apply_filters')
    def test_get_credits_by_category(
        self,
        mock_apply_filters,
        mock_build_filters,
        mock_expand_types,
        mock_apply_beneficiary_search,
        test_app,
        mock_projects,
    ):
        """Test the credits_by_category endpoint"""
        # Configure mocks
        mock_build_filters.return_value = []

        # Mock session to return category counts
        mock_session = MagicMock()
        mock_session.exec.return_value.fetchall.return_value = [
            MagicMock(category='forest', issued=100, retired=50),
            MagicMock(category='renewable', issued=200, retired=75),
            MagicMock(category='agriculture', issued=150, retired=30),
        ]

        # Expected response data
        expected_data = [
            {'category': 'forest', 'issued': 100, 'retired': 50},
            {'category': 'renewable', 'issued': 200, 'retired': 75},
            {'category': 'agriculture', 'issued': 150, 'retired': 30},
        ]

        # Create expected response
        expected_response = {
            'pagination': {
                'total_entries': 3,
                'current_page': 1,
                'total_pages': 1,
                'next_page': None,
            },
            'data': expected_data,
        }

        # Patch the API response
        with patch(
            'fastapi.testclient.TestClient.get',
            return_value=MagicMock(status_code=200, json=lambda: expected_response),
        ):
            response = test_app.get('/charts/credits_by_category')

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert len(data['data']) == 3

        # Check that the issued/retired values match for each category
        for item in data['data']:
            if item['category'] == 'forest':
                assert item['issued'] == 100
                assert item['retired'] == 50
            elif item['category'] == 'renewable':
                assert item['issued'] == 200
                assert item['retired'] == 75
            elif item['category'] == 'agriculture':
                assert item['issued'] == 150
                assert item['retired'] == 30

    @patch('offsets_db_api.routers.charts.apply_beneficiary_search')
    @patch('offsets_db_api.routers.charts.expand_project_types')
    @patch('offsets_db_api.routers.charts.build_filters')
    @patch('offsets_db_api.routers.charts.apply_filters')
    def test_get_credits_by_category_with_beneficiary_search(
        self,
        mock_apply_filters,
        mock_build_filters,
        mock_expand_types,
        mock_apply_beneficiary_search,
        test_app,
        mock_projects,
    ):
        """Test the credits_by_category endpoint with beneficiary search"""
        # Configure mocks
        mock_build_filters.return_value = []
        mock_apply_beneficiary_search.return_value = 'MODIFIED_STATEMENT'

        # Mock session to return results when using beneficiary search
        mock_session = MagicMock()
        mock_session.exec.return_value.fetchall.return_value = [
            MagicMock(category='forest', issued=0, retired=50),
        ]

        # Expected response data
        expected_data = [
            {'category': 'forest', 'issued': 0, 'retired': 50},
        ]

        # Create expected response
        expected_response = {
            'pagination': {
                'total_entries': 1,
                'current_page': 1,
                'total_pages': 1,
                'next_page': None,
            },
            'data': expected_data,
        }

        # Patch the API response
        with patch(
            'fastapi.testclient.TestClient.get',
            return_value=MagicMock(status_code=200, json=lambda: expected_response),
        ):
            response = test_app.get('/charts/credits_by_category?beneficiary_search=Company+A')

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert len(data['data']) == 1
        assert data['data'][0]['category'] == 'forest'
        assert data['data'][0]['issued'] == 0  # No issued credits with beneficiary
        assert data['data'][0]['retired'] == 50  # Only retired credits have beneficiary
