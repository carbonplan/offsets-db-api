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
    logger.info(f'Loading file with kwargs: {kwargs}')
    return pd.read_csv(file_url, **kwargs)


def generate_hash(df: pd.DataFrame | str) -> str:
    """Generate a hash of the dataframe contents"""

    if isinstance(df, pd.DataFrame):
        # Convert the DataFrame to a byte string.
        byte_str: bytes = df.to_json().encode()

    else:
        # Convert the string to a byte string.
        byte_str = df.encode()

    # Compute the hash of the byte string.
    return hashlib.sha256(byte_str).hexdigest()


def process_files(*, session: Session, files: list[File]):
    """Process a file and update its status in the database"""

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


def process_project_records(*, session: Session, model: Project, file: File) -> None:
    """Process project records in a file"""
    try:
        sha_hash = ''
        kwargs = {}
        if file.chunksize is not None and file.chunksize > 0:
            kwargs['chunksize'] = file.chunksize
        else:
            kwargs['chunksize'] = 10_000

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
                )

            if new_records := find_new_records(
                existing_records=existing_records,
                records=records,
                attribute_name=attribute_names[file.category],
            ):
                insert_new_records(session=session, model=model, new_records=new_records)

            if ids_to_delete := find_ids_to_delete(
                existing_records=existing_records,
                records=records,
                attribute_name=attribute_names[file.category],
            ):
                delete_records(
                    session=session,
                    model=model,
                    ids_to_delete=ids_to_delete,
                    attribute_name=attribute_names[file.category],
                )
            sha_hash += generate_hash(df)
        sha = generate_hash(sha_hash)
        update_file_status(session=session, file=file, content_sha=sha)

    except Exception as exc:
        session.rollback()
        logger.error(f'Error processing file: {file}. Reason: {exc}')
        raise exc


def find_existing_records(
    *, session: Session, model: Project, attribute_name: str, records: list[dict]
) -> list[Project]:
    """Find existing records in the database"""
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
    existing_records: list[Project],
    records: list[dict],
    attribute_name: str,
    keys: list[str],
    batch_size: int = 5000,
) -> None:
    """Update existing records if they are also present in the loaded records"""
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
    """Insert new records into the database"""
    if new_records:
        for i in range(0, len(new_records), batch_size):
            batch = new_records[i : i + batch_size]
            logger.info(f'Inserting batch {i} - {i + batch_size}')
            session.bulk_insert_mappings(model, batch)
            session.commit()
            logger.info(f'Inserted batch {i} - {i + batch_size}')


def find_ids_to_delete(
    *, existing_records: list[Project], records: list[dict], attribute_name: str
) -> set:
    """Find ids of records that are in the database but not in the loaded records"""
    existing_ids = {getattr(record, attribute_name) for record in existing_records}
    loaded_ids = {record[attribute_name] for record in records}
    ids_to_delete = existing_ids - loaded_ids
    logger.info(f'Found {len(ids_to_delete)} ids to delete')
    return ids_to_delete


def delete_records(
    *,
    session: Session,
    model: Project,
    ids_to_delete: set,
    attribute_name: str,
    batch_size: int = 5000,
) -> None:
    """Delete records from the database in batches"""
    if ids_to_delete:
        ids_to_delete_list = list(ids_to_delete)
        for i in range(0, len(ids_to_delete_list), batch_size):
            batch = ids_to_delete_list[i : i + batch_size]
            logger.info(f'Deleting batch {i} - {i + batch_size}')
            statement = delete(model).where(getattr(model, attribute_name).in_(batch))
            session.execute(statement)
            session.commit()
            logger.info(f'Deleted batch {i} - {i + batch_size}')


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
        session.query(Project.registry, Credit.transaction_type, func.count(Credit.id))
        .join(Project, Credit.project_id == Project.project_id)
        .group_by(Project.registry, Credit.transaction_type)
        .all()
    )

    for registry, transaction_type, total_credits in credit_registry_transaction_type_counts:
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
                f'🔄 Updating existing stats for registry {registry}, transaction type {transaction_type} with {total_credits} total credits.'
            )
            credit_stats.total_credits = total_credits
        except NoResultFound:
            logger.info(
                f'➕ Adding new stats for registry {registry}, transaction type {transaction_type} with {total_credits} total credits.'
            )
            credit_stats = CreditStats(
                date=date,
                registry=registry,
                transaction_type=transaction_type,
                total_credits=total_credits,
            )
            session.add(credit_stats)

    session.commit()
    logger.info(f'✅ Credit stats updated successfully for {date}.')
