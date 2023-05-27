import hashlib
import traceback

import numpy as np
import pandas as pd
from sqlmodel import Session, delete, select

from .logging import get_logger
from .models import Credit, File, Project

# This dictionary maps each file category to the model class that can be used to process the file.

models = {'projects': Project, 'credits': Credit}
attribute_names = {'projects': 'project_id', 'credits': 'transaction_serial_number'}
keys_mapping = {'projects': ['id', 'project_id'], 'credits': ['id', 'transaction_serial_number']}

logger = get_logger()


def load_csv_file(file_url: str, **kwargs) -> pd.DataFrame:
    df = pd.read_csv(file_url, **kwargs)
    # Sort columns in ascending order
    df = df[df.columns.sort_values()]
    logger.info(f'Successfully loaded file: {file_url}')
    logger.info(f'File has {len(df)} rows')
    return df


def generate_hash(df: pd.DataFrame) -> str:
    """Generate a hash of the dataframe contents"""

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f'Expected pandas DataFrame, got {type(df)}')

    # Convert the DataFrame to a byte string.
    byte_str: bytes = df.to_json().encode()

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


def process_project_records(session: Session, model: Project, file: File) -> None:
    """Process project records in a file"""
    try:
        df = load_csv_file(file.url)

        # Convert NaN values to None and convert dataframe to a list of dicts
        records = df.replace({np.nan: None}).to_dict('records')
        logger.debug(f'Found {len(records)} records in file')

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

        new_records = find_new_records(
            existing_records=existing_records,
            records=records,
            attribute_name=attribute_names[file.category],
        )

        if new_records:
            insert_new_records(session=session, model=model, new_records=new_records)

        ids_to_delete = find_ids_to_delete(
            existing_records=existing_records,
            records=records,
            attribute_name=attribute_names[file.category],
        )

        if ids_to_delete:
            delete_records(
                session=session,
                model=model,
                ids_to_delete=ids_to_delete,
                attribute_name=attribute_names[file.category],
            )

        update_file_status(session=session, file=file, df=df)

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
) -> None:
    """Update existing records if they are also present in the loaded records"""
    records_to_update = []
    for existing_record in existing_records:
        matching_record = next(
            (
                rec
                for rec in records
                if rec[attribute_name] == getattr(existing_record, attribute_name)
            ),
            None,
        )
        if matching_record:
            update_record(
                existing_record=existing_record, matching_record=matching_record, keys=keys
            )
            records_to_update.append(existing_record.dict())
    if records_to_update:
        logger.info(f'Updating {len(records_to_update)} existing records')
        session.bulk_update_mappings(model, records_to_update)
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


def insert_new_records(session: Session, model: Project, new_records: list[dict]) -> None:
    """Insert new records into the database"""
    if new_records:
        session.bulk_insert_mappings(model, new_records)
        session.commit()


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
    *, session: Session, model: Project, ids_to_delete: set, attribute_name: str
) -> None:
    """Delete records from the database"""
    if ids_to_delete:
        statement = delete(model).where(getattr(model, attribute_name).in_(ids_to_delete))
        session.execute(statement)


def update_file_status(session: Session, file: File, df: pd.DataFrame) -> None:
    """Update and commit File object to database"""
    file.content_hash = generate_hash(df)
    file.status = 'success'
    session.add(file)
    session.commit()
    session.refresh(file)
    logger.info(f'Done processing file: {file}')
