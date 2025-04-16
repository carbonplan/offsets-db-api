import datetime
import os
import tempfile
from unittest import mock

import pandas as pd
import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from offsets_db_api.models import File
from offsets_db_api.tasks import (
    ensure_projects_exist,
    process_dataframe,
    process_files,
    update_file_status,
)


# Fixtures for mock objects and sample data
@pytest.fixture
def mock_session():
    """Create a mock SQLModel session"""
    return mock.MagicMock(spec=Session)


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine"""
    engine = mock.MagicMock()

    # Mock connection context manager
    mock_connection = mock.MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    # Mock has_table method
    mock_dialect = mock.MagicMock()
    engine.dialect = mock_dialect
    mock_dialect.has_table.return_value = True

    return engine


@pytest.fixture
def sample_file_project():
    """Create a sample project file"""
    return File(
        id=1,
        url='s3://bucket/projects.parquet',
        category='projects',
        status='pending',
        error=None,
        recorded_at=datetime.datetime.now(datetime.timezone.utc),
    )


@pytest.fixture
def sample_file_credit():
    """Create a sample credit file"""
    return File(
        id=2,
        url='s3://bucket/credits.parquet',
        category='credits',
        status='pending',
        error=None,
        recorded_at=datetime.datetime.now(datetime.timezone.utc),
    )


@pytest.fixture
def sample_file_clip():
    """Create a sample clip file"""
    return File(
        id=3,
        url='s3://bucket/clips.parquet',
        category='clips',
        status='pending',
        error=None,
        recorded_at=datetime.datetime.now(datetime.timezone.utc),
    )


@pytest.fixture
def sample_df_projects():
    """Create a sample project DataFrame"""
    return pd.DataFrame(
        {
            'project_id': ['VCS001', 'GS002'],
            'name': ['Test Project 1', 'Test Project 2'],
            'registry': ['verra', 'gold-standard'],
            'proponent': ['Org1', 'Org2'],
            'protocol': [['test-protocol'], ['test-protocol']],
            'category': ['forestry', 'renewable'],
            'type': ['REDD', 'Solar'],
            'type_source': ['carbonplan', 'carbonplan'],
            'status': ['active', 'active'],
            'country': ['USA', 'Canada'],
            'listed_at': [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
            'is_compliance': [False, False],
            'retired': [1000, 2000],
            'issued': [5000, 6000],
            'project_url': ['http://test1.com', 'http://test2.com'],
        }
    )


@pytest.fixture
def sample_df_credits():
    """Create a sample credit DataFrame"""
    return pd.DataFrame(
        {
            'project_id': ['VCS001', 'GS002', 'VCS001'],
            'quantity': [1000, 2000, 3000],
            'vintage': [2020, 2021, 2022],
            'transaction_date': [
                datetime.date(2021, 1, 1),
                datetime.date(2022, 1, 1),
                datetime.date(2023, 1, 1),
            ],
            'transaction_type': ['issuance', 'retirement', 'issuance'],
            'retirement_account': [None, 'Acct123', None],
            'retirement_reason': [None, 'Offsetting', None],
            'retirement_note': [None, 'Note', None],
            'retirement_beneficiary': [None, 'Company XYZ', None],
            'retirement_beneficiary_harmonized': [None, 'XYZ Inc', None],
            'recorded_at': [
                datetime.datetime(2021, 6, 1),
                datetime.datetime(2022, 6, 1),
                datetime.datetime(2023, 6, 1),
            ],
        }
    )


@pytest.fixture
def sample_df_clips():
    """Create a sample clip DataFrame"""
    return pd.DataFrame(
        {
            'name': ['Clip 1', 'Clip 2'],
            'description': ['Test clip 1', 'Test clip 2'],
            'tags': [['tag1', 'tag2'], ['tag3']],
            'project_ids': [['VCS001', 'GS002'], ['VCS001']],
        }
    )


@pytest.fixture
def mock_watchdog_file():
    """Create a temporary watch_dog_file"""
    fd, path = tempfile.mkstemp()
    os.close(fd)
    yield path
    os.unlink(path)


def test_update_file_status_success(mock_session, sample_file_project):
    # Test updating a file status to success
    update_file_status(sample_file_project, mock_session, 'success')

    assert sample_file_project.status == 'success'
    assert sample_file_project.error is None
    mock_session.add.assert_called_once_with(sample_file_project)
    mock_session.commit.assert_called_once()
    mock_session.refresh.assert_called_once_with(sample_file_project)


def test_update_file_status_failure(mock_session, sample_file_project):
    # Test updating a file status to failure with an error message
    error_msg = 'Test error message'
    update_file_status(sample_file_project, mock_session, 'failure', error=error_msg)

    assert sample_file_project.status == 'failure'
    assert sample_file_project.error == error_msg
    mock_session.add.assert_called_once_with(sample_file_project)
    mock_session.commit.assert_called_once()
    mock_session.refresh.assert_called_once_with(sample_file_project)


# Tests for ensure_projects_exist function
def test_ensure_projects_exist_no_missing_ids(mock_session, sample_df_credits):
    # Setup: All project IDs already exist
    mock_exec = mock_session.exec.return_value
    mock_exec.all.return_value = sample_df_credits['project_id'].unique().tolist()
    ensure_projects_exist(sample_df_credits, mock_session)

    mock_session.add.assert_not_called()
    mock_session.commit.assert_called_once()


def test_ensure_projects_exist_with_missing_ids(mock_session, sample_df_credits):
    # Setup: Some project IDs are missing
    mock_exec = mock_session.exec.return_value
    mock_exec.all.return_value = [
        sample_df_credits['project_id'].unique()[0]
    ]  # Only return first ID

    # Run function
    with mock.patch('offsets_db_api.tasks.get_registry_from_project_id') as mock_get_registry:
        mock_get_registry.return_value = 'verra'
        ensure_projects_exist(sample_df_credits, mock_session)

    # Assert
    # Should be called once for the missing ID
    assert mock_session.bulk_insert_mappings.call_count == 1
    mock_session.commit.assert_called_once()


def test_ensure_projects_exist_integrity_error(mock_session, sample_df_credits):
    mock_exec = mock_session.exec.return_value
    mock_exec.all.return_value = []  # No IDs exist
    mock_session.commit.side_effect = IntegrityError('statement', 'params', 'orig')

    with mock.patch('offsets_db_api.tasks.get_registry_from_project_id') as mock_get_registry:
        mock_get_registry.return_value = 'verra'
        with pytest.raises(IntegrityError):
            ensure_projects_exist(sample_df_credits, mock_session)

    mock_session.rollback.assert_called_once()


def test_process_dataframe(mock_engine, sample_df_projects):
    table_name = 'project'

    with mock.patch('offsets_db_api.tasks.get_session') as mock_get_session:
        mock_next_session = mock.MagicMock()
        mock_get_session.return_value.__next__.return_value = mock_next_session

        # Mock the to_sql method before calling process_dataframe
        with mock.patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            process_dataframe(sample_df_projects, table_name, mock_engine)

            # Now check the mock was called correctly
            mock_to_sql.assert_called_once()
            args, kwargs = mock_to_sql.call_args
            assert args[0] == table_name
            assert kwargs.get('if_exists') == 'append'
            assert kwargs.get('index') is False

    conn = mock_engine.begin.return_value.__enter__.return_value
    conn.execute.assert_called_once()  # Should execute TRUNCATE statement


def test_process_dataframe_credit_table(mock_engine, sample_df_credits):
    table_name = 'credit'

    with (
        mock.patch('offsets_db_api.tasks.get_session') as mock_get_session,
        mock.patch('offsets_db_api.tasks.ensure_projects_exist') as mock_ensure_projects,
    ):
        mock_next_session = mock.MagicMock()
        mock_get_session.return_value.__next__.return_value = mock_next_session

        process_dataframe(sample_df_credits, table_name, mock_engine)

    mock_ensure_projects.assert_called_once_with(sample_df_credits, mock_next_session)


@pytest.mark.asyncio
async def test_process_files_order(
    mock_engine, mock_session, sample_file_project, sample_file_credit
):
    # Test that project files are processed first
    files = [sample_file_credit, sample_file_project]  # Credits before projects

    with (
        mock.patch('pandas.read_parquet') as mock_read_parquet,
        mock.patch('offsets_db_api.tasks.project_schema') as mock_project_schema,
        mock.patch('offsets_db_api.tasks.credit_schema') as mock_credit_schema,
        mock.patch('offsets_db_api.tasks.process_dataframe') as _,
        mock.patch('offsets_db_api.tasks.update_file_status') as _,
    ):
        # Setup mock returns
        mock_read_parquet.side_effect = [
            mock.MagicMock(),  # For project file
            mock.MagicMock(),  # For credit file
        ]
        mock_project_schema.validate.return_value = pd.DataFrame()
        mock_credit_schema.validate.return_value = pd.DataFrame()

        # Run function
        await process_files(engine=mock_engine, session=mock_session, files=files)

        # Check order of calls to read_parquet
        assert mock_read_parquet.call_args_list[0][0][0] == sample_file_project.url
        assert mock_read_parquet.call_args_list[1][0][0] == sample_file_credit.url


@pytest.mark.asyncio
async def test_process_files_project(
    mock_engine, mock_session, sample_file_project, sample_df_projects
):
    # Test processing a project file
    with (
        mock.patch('pandas.read_parquet') as mock_read_parquet,
        mock.patch('offsets_db_api.tasks.project_schema') as mock_project_schema,
        mock.patch('offsets_db_api.tasks.process_dataframe') as mock_process_df,
        mock.patch('offsets_db_api.tasks.update_file_status') as mock_update_status,
    ):
        mock_read_parquet.return_value = sample_df_projects
        mock_project_schema.validate.return_value = sample_df_projects

        await process_files(engine=mock_engine, session=mock_session, files=[sample_file_project])

        mock_read_parquet.assert_called_once_with(sample_file_project.url, engine='fastparquet')
        mock_project_schema.validate.assert_called_once_with(sample_df_projects)
        mock_process_df.assert_called_once()
        mock_update_status.assert_called_once_with(sample_file_project, mock_session, 'success')


@pytest.mark.asyncio
async def test_process_files_credit_integrated(
    mock_engine, mock_session, sample_file_credit, sample_df_credits
):
    # Test processing a credit file with minimal mocking
    with (
        mock.patch('pandas.read_parquet', return_value=sample_df_credits),
        mock.patch('offsets_db_api.tasks.credit_schema') as mock_credit_schema,
        mock.patch('offsets_db_api.tasks.process_dataframe') as mock_process_df,
        mock.patch('offsets_db_api.tasks.update_file_status') as mock_update_status,
    ):
        # This approach lets the actual DataFrame methods run
        mock_credit_schema.validate.return_value = sample_df_credits

        await process_files(engine=mock_engine, session=mock_session, files=[sample_file_credit])

        mock_credit_schema.validate.assert_called_once()
        mock_process_df.assert_called_once()
        mock_update_status.assert_called_once_with(sample_file_credit, mock_session, 'success')


@pytest.mark.asyncio
async def test_process_files_clips(mock_engine, mock_session, sample_file_clip, sample_df_clips):
    # Test processing a clip file
    with (
        mock.patch('pandas.read_parquet') as mock_read_parquet,
        mock.patch('offsets_db_api.tasks.clip_schema') as mock_clip_schema,
        mock.patch('offsets_db_api.tasks.process_dataframe') as mock_process_df,
        mock.patch('offsets_db_api.tasks.update_file_status') as mock_update_status,
        mock.patch('offsets_db_api.tasks.watch_dog_file', 'test_watchdog'),
    ):
        mock_read_parquet.return_value = sample_df_clips
        mock_clip_schema.validate.return_value = pd.DataFrame(
            {
                'id': [0, 1],
                'name': ['Clip 1', 'Clip 2'],
                'description': ['Test clip 1', 'Test clip 2'],
                'tags': [['tag1', 'tag2'], ['tag3']],
                'project_ids': [['VCS001', 'GS002'], ['VCS001']],
            }
        )

        # Mock open file
        mock_open = mock.mock_open()
        with mock.patch('builtins.open', mock_open):
            await process_files(engine=mock_engine, session=mock_session, files=[sample_file_clip])

        # Check clip processing
        assert mock_process_df.call_count == 2  # Once for clip, once for clipproject
        mock_open.assert_called_once_with('test_watchdog', 'w')
        mock_update_status.assert_called_once_with(sample_file_clip, mock_session, 'success')


@pytest.mark.asyncio
async def test_process_files_error_handling(mock_engine, mock_session, sample_file_credit):
    # Test error handling during file processing
    with (
        mock.patch('pandas.read_parquet') as mock_read_parquet,
        mock.patch('offsets_db_api.tasks.update_file_status') as mock_update_status,
    ):
        # Simulate an exception when reading the parquet file
        mock_read_parquet.side_effect = Exception('Test error')

        await process_files(engine=mock_engine, session=mock_session, files=[sample_file_credit])

        # Check that update_file_status was called with failure status
        mock_update_status.assert_called_once()
        args = mock_update_status.call_args[0]
        kwargs = mock_update_status.call_args[1]
        assert args[0] == sample_file_credit
        assert args[1] == mock_session
        assert args[2] == 'failure'
        assert 'Test error' in kwargs['error']
