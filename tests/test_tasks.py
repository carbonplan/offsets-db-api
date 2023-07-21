import datetime
import hashlib
import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlmodel import Session

from carbonplan_offsets_db.models import CreditStats, File, Project, ProjectStats
from carbonplan_offsets_db.tasks import (
    generate_hash,
    process_files,
    process_project_records,
    update_credit_stats,
    update_project_stats,
)


@pytest.fixture
def mock_session():
    return MagicMock(spec=Session)


def test_generate_hash_with_dataframe():
    # Create a simple test DataFrame
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})

    # Test that generate_hash() produces expected results
    expected_hash = hashlib.sha256(df.to_json().encode()).hexdigest()
    assert generate_hash(df) == expected_hash


def test_process_project_records_calls_load_csv_file_with_correct_url(mock_session):
    project_file = MagicMock(
        url='https://example.com/projects.csv', category='projects', chunksize=10_000
    )

    df = pd.DataFrame([project.dict() for project in generate_mock_projects()])

    with patch('carbonplan_offsets_db.tasks.load_csv_file') as mock_load_csv_file:
        # Mock the return value of load_csv_file
        # Instead of returning a single buffer, we return a list of buffers, each representing a chunk of the file
        mock_load_csv_file.return_value = iter([df])

        # Call process_project_records function with the mocked objects
        process_project_records(
            session=mock_session,
            file=project_file,
            model=Project,
        )

        # Assert that load_csv_file was called with the correct URL
        mock_load_csv_file.assert_called_once_with(project_file.url, chunksize=10_000)


def test_process_files_calls_process_project_records_with_correct_args_for_project_files(
    mock_session,
):
    project_files = [MagicMock(url='https://example.com/projects.csv', category='projects')]

    with patch(
        'carbonplan_offsets_db.tasks.process_project_records'
    ) as mock_process_project_records:
        # Call process_files function with the mocked objects
        process_files(session=mock_session, files=project_files)

        # Assert that process_project_records was called with the correct arguments
        mock_process_project_records.assert_called_once_with(
            session=mock_session,
            file=project_files[0],
            model=Project,
        )


def test_process_files_handles_unknown_file_categories(mock_session, caplog):
    unknown_files = [MagicMock(url='https://example.com/unknown.csv', category='unknown')]

    # Call process_files function with the mocked objects and capture logs
    with caplog.at_level(logging.INFO):
        process_files(session=mock_session, files=unknown_files)

    # Assert that logger.info was called with the correct message
    assert (
        f'Unknown file category: {unknown_files[0].category}. Skipping file {unknown_files[0].url}'
        in caplog.text
    )


def test_process_files_catches_and_handles_exceptions(mock_session):
    project_files = [MagicMock(url='https://example.com/projects.csv', category='projects')]

    with patch('carbonplan_offsets_db.tasks.traceback.format_exc') as mock_format_exc:
        # Set up the mock to simulate an exception being raised
        mock_format_exc.return_value = 'Traceback (most recent call last):\n File "foo.py", line 10, in <module>\n bar()\nValueError: invalid argument'

        # Call process_files function with a single project file
        process_files(session=mock_session, files=project_files)

        # Assert that an error message is logged and the file status is set to 'failure'
        assert project_files[0].status == 'failure'
        assert project_files[0].error == 'ValueError: invalid argument'


def generate_mock_projects(num_projects=3):
    projects = []
    for i in range(num_projects):
        project = Project(
            id=i,
            project_id=f'project_id_{i}',
            name=f'Project {i}',
            registry='Registry',
            proponent=None,
            protocol=None,
            developer=None,
            voluntary_status=None,
            country=None,
            started_at=None,
            registered_at=None,
            recorded_at=datetime.datetime.now(datetime.timezone.utc),
            description=None,
            details_url=None,
        )
        projects.append(project)
    return projects


def generate_mock_file(file_id=1, file_url='http://example.com/file.csv'):
    return File(
        id=file_id,
        url=file_url,
        content_hash=None,
        status='pending',
        error=None,
        recorded_at=datetime.datetime.now(datetime.timezone.utc),
        category='projects',
    )


@patch('carbonplan_offsets_db.tasks.load_csv_file')
def test_insert(mock_load_csv_file, mock_session):
    df = pd.DataFrame([project.dict() for project in generate_mock_projects()])
    mock_load_csv_file.return_value = iter([df])  # Set your mocked dataframe

    # Mock the database query result with no existing records
    mock_session.exec.return_value.all.return_value = []

    file = generate_mock_file()
    process_project_records(
        session=mock_session,
        file=file,
        model=Project,
    )

    # Check if the insert operation was called with the expected number of new records
    assert mock_session.bulk_insert_mappings.call_count == 1
    inserted_records = mock_session.bulk_insert_mappings.call_args[0][1]
    assert len(inserted_records) == len(df)  # The expected number of new records
    assert file.status == 'success'  # The file status should be set to 'success'
    assert file.content_hash is not None  # The file content hash should be set


@patch('carbonplan_offsets_db.tasks.load_csv_file')
def test_update(mock_load_csv_file, mock_session):
    df = pd.DataFrame([project.dict() for project in generate_mock_projects()])
    # add dummy updated values to the dataframe
    df['recorded_at'] = '2021-01-01'
    mock_load_csv_file.return_value = iter([df])  # Set your mocked dataframe

    # Mock the database query result with existing records
    mock_session.exec.return_value.all.return_value = generate_mock_projects()

    file = generate_mock_file()
    process_project_records(
        session=mock_session,
        file=file,
        model=Project,
    )

    # Check if the update operation was called with the expected number of updated records
    assert mock_session.bulk_update_mappings.call_count == 1
    updated_records = mock_session.bulk_update_mappings.call_args[0][1]
    assert len(updated_records) == len(df)  # The expected number of updated records
    assert file.status == 'success'  # The file status should be set to 'success'


def test_update_project_stats(test_db_session):
    # Call the function you want to test
    session = next(test_db_session)
    update_project_stats(session=session)

    # Check if the data was properly updated

    result = session.query(ProjectStats).first()

    # Assertions to check if the values are updated correctly
    assert result is not None
    assert result.total_projects > 0


def test_update_credit_stats(test_db_session):
    session = next(test_db_session)
    # Call the function you want to test
    update_credit_stats(session=session)

    # Check if the data was properly updated
    result = session.query(CreditStats).first()

    # Assertions to check if the values are updated correctly
    assert result is not None
    assert result.total_credits > 0
