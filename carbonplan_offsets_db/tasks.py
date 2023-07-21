import datetime
import hashlib
import traceback

import numpy as np
import pandas as pd
from sqlalchemy.orm.exc import NoResultFound
from sqlmodel import Session, delete, func, select

from .database import get_session
from .logging import get_logger
from .models import Credit, CreditStats, File, Project, ProjectStats

# This dictionary maps each file category to the model class that can be used to process the file.

models = {'projects': Project, 'credits': Credit}
attribute_names = {'projects': 'project_id', 'credits': 'transaction_serial_number'}
keys_mapping = {'projects': ['id', 'project_id'], 'credits': ['id', 'transaction_serial_number']}

logger = get_logger()


def load_csv_file(file_url: str, **kwargs) -> pd.DataFrame:
    """
    Load a CSV file from a URL.

    Parameters
    ----------
    file_url : str
        URL of the file to load.
    **kwargs : dict
        Additional keyword arguments to pass to pandas.read_csv.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the file data.
    """
    logger.info(f'📚 Loading file with kwargs: {kwargs}')
    return pd.read_csv(file_url, **kwargs)


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

                if file.valid_records_file_url:
                    df = load_csv_file(file.valid_records_file_url)
                    valid_ids = df[attribute_names[file.category]].tolist()
                    remove_stale_records(
                        session=session,
                        model=models[file.category],
                        attribute_name=attribute_names[file.category],
                        valid_ids=valid_ids,
                    )

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
        sha_hash = ''
        kwargs = {}
        if file.chunksize is not None and file.chunksize > 0:
            kwargs['chunksize'] = file.chunksize
        else:
            kwargs['chunksize'] = 10_000

        batch_size = kwargs['chunksize']

        chunks = load_csv_file(file.url, **kwargs)
        logger.info(f'Processing {file.category} file {file.url}')
        for df in chunks:
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f'Expected a DataFrame, got {type(df)}')

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
            sha_hash += generate_hash(df)
        sha = generate_hash(sha_hash)
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


def update_project_stats(session: Session = None):
    date = datetime.date.today()
    logger.info(f'📅 Updating project stats for {date}...')

    if not session:
        session = next(get_session())

    project_registry_counts = (
        session.query(Project.registry, func.count(Project.id)).group_by(Project.registry).all()
    )

    for registry, total_projects in project_registry_counts:
        try:
            project_stats = (
                session.query(ProjectStats)
                .filter(ProjectStats.date == date, ProjectStats.registry == registry)
                .one()
            )

            logger.info(
                f'🔄 Updating existing stats for registry {registry} with {total_projects} total projects.'
            )
            project_stats.total_projects = total_projects
        except NoResultFound:
            logger.info(
                f'➕ Adding new stats for registry {registry} with {total_projects} total projects.'
            )
            project_stats = ProjectStats(
                date=date, registry=registry, total_projects=total_projects
            )
            session.add(project_stats)

    session.commit()
    logger.info(f'✅ Project stats updated successfully for {date}.')


def update_credit_stats(session: Session = None):
    date = datetime.date.today()
    logger.info(f'📅 Updating credit stats for {date}...')
    if not session:
        session = next(get_session())

    credit_registry_transaction_type_counts = (
        session.query(
            Project.registry,
            Credit.transaction_type,
            func.sum(Credit.quantity),
            func.count(Credit.id),
        )
        .join(Project, Credit.project_id == Project.project_id)
        .group_by(Project.registry, Credit.transaction_type)
        .all()
    )

    for (
        registry,
        transaction_type,
        total_credits,
        total_transactions,
    ) in credit_registry_transaction_type_counts:
        try:
            credit_stats = (
                session.query(CreditStats)
                .filter(
                    CreditStats.date == date,
                    CreditStats.registry == registry,
                    CreditStats.transaction_type == transaction_type,
                )
                .one()
            )

            logger.info(
                f'🔄 Updating existing stats for registry {registry}, transaction type {transaction_type} with {total_credits} total credits from {total_transactions} total transactions.'
            )
            credit_stats.total_credits = total_credits
            credit_stats.total_transactions = total_transactions
        except NoResultFound:
            logger.info(
                f'➕ Adding new stats for registry {registry}, transaction type {transaction_type} with {total_credits} total credits from {total_transactions} total transactions.'
            )
            credit_stats = CreditStats(
                date=date,
                registry=registry,
                transaction_type=transaction_type,
                total_credits=total_credits,
                total_transactions=total_transactions,
            )
            session.add(credit_stats)

    session.commit()
    logger.info(f'✅ Credit stats updated successfully for {date}.')


def export_table_to_csv(*, table, path: str, session: Session = None):
    """
    Export a table from the database to a CSV file.

    table: pydantic.BaseModel
        The SQLAlchemy table class of the table to export.
    path: str
        The path of the file to which the table should be exported.
    session: Session
        An optional SQLAlchemy session. If not provided, a new session will be created.
    """

    import datetime
    import gc

    import upath

    today = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')

    # Create the filepath
    directory = upath.UPath(path) / today
    directory.mkdir(parents=True, exist_ok=True)
    filepath = f'{directory}/{table.__name__.lower()}s.csv.gz'

    # If no session is provided, create a new one
    if not session:
        session = next(get_session())

    # Log the start of the operation
    logger.info(f'🚀 Starting export of table {table.__name__} to {filepath}...')

    # Execute a SELECT * query on the table
    rows = session.execute(select(table)).fetchall()

    # If fetchall() returns nothing, log the event and return
    if not rows:
        logger.info(f'🚧 No data found in table {table.__name__}. No CSV file will be created.')
        return

    # Convert the result rows to a list of dictionaries
    rows_as_dicts = [row[0].dict() for row in rows]

    # Create a pandas DataFrame from the list of dictionaries
    df = pd.DataFrame(rows_as_dicts)

    # Log the first few rows of the DataFrame
    logger.info(f"👀 Here's a sneak peek of the data:\n{df.head()}")

    # Write the DataFrame to a CSV file
    df.to_csv(filepath, index=False)

    # Log the completion of the operation
    logger.info(f'✅ Successfully exported table {table.__name__} to {filepath}!')

    # Delete the DataFrame to free up memory
    del df

    # Force Python's garbage collector to release unreferenced memory
    gc.collect()

    # Log the completion of the garbage collection
    logger.info('🗑️ Garbage collected!')
