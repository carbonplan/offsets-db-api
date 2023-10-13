import hashlib
import traceback

import numpy as np
import pandas as pd
from sqlmodel import Session, delete, select

from .database import get_session
from .logging import get_logger
from .models import Credit, File, Project

# This dictionary maps each file category to the model class that can be used to process the file.

models = {'projects': Project, 'credits': Credit}
attribute_names = {'projects': 'project_id', 'credits': 'transaction_serial_number'}
keys_mapping = {'projects': ['id', 'project_id'], 'credits': ['id', 'transaction_serial_number']}

logger = get_logger()


def generate_hash(df: pd.DataFrame | str) -> str:
    """
    Generate a SHA256 hash of a DataFrame or string.

    Parameters
    ----------
    df : pd.DataFrame | str
        DataFrame or string to hash.

    Returns
    -------
    str
        SHA256 hash of the input.
    """
    if isinstance(df, pd.DataFrame):
        byte_str: bytes = df.to_json().encode()  # Convert DataFrame to byte string
    else:
        byte_str = df.encode()  # Convert string to byte string

    # Compute and return SHA256 hash
    return hashlib.sha256(byte_str).hexdigest()


def process_files(*, session: Session, files: list[File]):
    """
    Process a list of files and update their status in the database.

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object.
    files : list of File
        List of File objects to process.
    """

    for file in files:
        try:
            if file.category in models:
                process_project_records(session=session, model=models[file.category], file=file)

            else:
                logger.info('Unknown file category: %s. Skipping file %s', file.category, file.url)

        except Exception:
            logger.error('Failed to process file: %s', file)
            trace = traceback.format_exc()
            logger.error(trace)
            file.status = 'failure'
            file.error = trace.splitlines()[-1]
            session.add(file)
            session.commit()
            session.refresh(file)


def process_project_records(*, session: Session, model: Project | Credit, file: File) -> None:
    """
    Process project records in a file.

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object.
    model : Project or Credit
        Model that will be used to process the file.
    file : File
        File object containing the data to be processed.

    Raises
    ------
    Exception
        If there is an error during processing, an exception is raised.
    """
    try:
        kwargs = {'chunksize': 10_000}

        batch_size = kwargs['chunksize']

        logger.info(f'📚 Loading file: {file.url}')
        df = pd.read_parquet(file.url)
        logger.info(f'Processing {file.category} file {file.url}')

        # Sort columns in ascending order
        df = df[df.columns.sort_values()]
        logger.info(f'File chunk has {len(df)} rows')
        # Convert NaN values to None and convert dataframe to a list of dicts
        records = df.replace({np.nan: None}).to_dict('records')
        logger.debug(f'Found {len(records)} records in file chunk')

        existing_records = find_existing_records(
            session=session,
            model=model,
            attribute_name=attribute_names[file.category],
            records=records,
        )

        if existing_records:
            update_existing_records(
                session=session,
                model=model,
                existing_records=existing_records,
                records=records,
                attribute_name=attribute_names[file.category],
                keys=keys_mapping[file.category],
                batch_size=batch_size,
            )

        if new_records := find_new_records(
            existing_records=existing_records,
            records=records,
            attribute_name=attribute_names[file.category],
        ):
            insert_new_records(
                session=session, model=model, new_records=new_records, batch_size=batch_size
            )
        sha = generate_hash(df)
        update_file_status(session=session, file=file, content_sha=sha)

    except Exception as exc:
        session.rollback()
        logger.error(f'Error processing file: {file}. Reason: {exc}')
        raise exc


def find_existing_records(
    *, session: Session, model: Project | Credit, attribute_name: str, records: list[dict]
) -> list[Project] | list[Credit]:
    """
    Find existing records in the database.

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object.
    model : Project, Credit
        Model that will be used to process the file.
    attribute_name : str
        Name of the attribute used to compare records.
    records : list[dict]
        List of dictionaries representing the records to find.

    Returns
    -------
    list[Project], list[Credit]
        List of existing project or credit records found in the database.
    """
    query = select(model).where(
        getattr(model, attribute_name).in_([r[attribute_name] for r in records])
    )
    existing_records = session.exec(query).all()
    existing_ids = {getattr(record, attribute_name) for record in existing_records}
    logger.info(f'Found {len(existing_ids)} existing records')
    return existing_records


def update_existing_records(
    *,
    session: Session,
    model: Project,
    existing_records: list[Project] | list[Credit],
    records: list[dict],
    attribute_name: str,
    keys: list[str],
    batch_size: int = 5000,
) -> None:
    """
    Update existing records if they are also present in the loaded records.

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object.
    model : Project, Credit
        Model that will be used to process the file.
    existing_records : list[Project]
        List of existing project records.
    records : list[dict]
        List of dictionaries representing the records to update.
    attribute_name : str
        Name of the attribute used to compare records.
    keys : list[str]
        List of keys used to update the records.
    batch_size : int, optional
        Size of the batch of records to be updated, by default 5000.
    """
    records_to_update = []
    for existing_record in existing_records:
        if matching_record := next(
            (
                rec
                for rec in records
                if rec[attribute_name] == getattr(existing_record, attribute_name)
            ),
            None,
        ):
            update_record(
                existing_record=existing_record, matching_record=matching_record, keys=keys
            )
            records_to_update.append(existing_record.dict())
    if records_to_update:
        logger.info(f'Updating {len(records_to_update)} existing records')
        for i in range(0, len(records_to_update), batch_size):
            batch = records_to_update[i : i + batch_size]
            logger.info(f'Updating batch {i} - {i + batch_size}')
            session.bulk_update_mappings(model, batch)
            session.commit()


def update_record(*, existing_record: Project, matching_record: dict, keys: list[str]) -> None:
    """Update an existing record with data from a matching record"""

    for key, value in matching_record.items():
        if key not in keys and hasattr(existing_record, key):
            setattr(existing_record, key, value)


def find_new_records(
    *, existing_records: list[Project], records: list[dict], attribute_name: str
) -> list[dict]:
    """Find new records that are not present in the database"""
    existing_ids = {getattr(record, attribute_name) for record in existing_records}
    new_records = [record for record in records if record[attribute_name] not in existing_ids]
    logger.info(f'Found {len(new_records)} new records')
    return new_records


def insert_new_records(
    session: Session, model: Project, new_records: list[dict], batch_size: int = 5000
) -> None:
    """
    Insert new records into the database.

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object.
    model : Project
        Model that will be used to process the file.
    new_records : list[dict]
        List of dictionaries representing the new records to be inserted.
    batch_size : int, optional
        Size of the batch of records to be inserted, by default 5000.
    """
    if new_records:
        for i in range(0, len(new_records), batch_size):
            batch = new_records[i : i + batch_size]
            logger.info(f'Inserting batch {i} - {i + batch_size}')
            session.bulk_insert_mappings(model, batch)
            session.commit()
            logger.info(f'Inserted batch {i} - {i + batch_size}')


def remove_stale_records(
    *, session: Session, model: Project | Credit, attribute_name: str, valid_ids: list[str]
):
    """
    Remove stale records from the project and credit tables.

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object.
    model : Project or Credit
        Model that will be used to process the file.
    attribute_name : str
        Name of the attribute used to compare records.
    valid_ids : list[str]
        List of valid ids.
    """

    try:
        logger.info(f'🗑️  Deleting stale records from {model.__name__} table')

        # Build a delete statement.

        stmt = delete(model).where(getattr(model, attribute_name).notin_(valid_ids))
        # Execute the statement.
        results = session.execute(stmt)
        # get count of records to delete
        count = results.rowcount

        # Commit the changes.
        session.commit()
        logger.info(f'✅  Deleted {count} stale records from {model.__name__} table')
    except Exception as e:
        logger.error(f'An error occurred: {str(e)}')
        session.rollback()


def update_file_status(session: Session, file: File, content_sha: str = None) -> None:
    """Update and commit File object to database"""
    file.content_hash = content_sha
    file.status = 'success'
    session.add(file)
    session.commit()
    session.refresh(file)
    logger.info(f'✅ Done processing file: {file}')


def calculate_totals(session: Session = None):
    """Calculate totals (issuances, retirements) for all projects in the database"""
    logger.info('🔄 Updating project retired and issued totals')
    # Start a new session
    if not session:
        session = next(get_session())

    try:
        # Fetch all projects
        projects = session.exec(select(Project)).all()

        # For each project...
        for project in projects:
            # Reset totals
            issued_total = 0
            retired_total = 0

            # Fetch all credits for this project
            credits = session.exec(
                select(Credit).where(Credit.project_id == project.project_id)
            ).all()

            # For each credit...
            for credit in credits:
                # Add to totals
                if credit.transaction_type == 'issuance':
                    issued_total += credit.quantity
                elif 'retirement' in credit.transaction_type:
                    retired_total += credit.quantity
            # Update totals for this project
            project.issued = issued_total
            project.retired = retired_total
            session.add(project)

        # Commit changes
        session.commit()
        logger.info('✅ Done updating project retired and issued totals')

    except Exception as exc:
        logger.error('Error updating project retired and issued totals')
        logger.exception(exc)
        session.rollback()
        raise exc

    finally:
        session.close()
