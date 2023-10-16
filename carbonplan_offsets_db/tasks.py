import traceback

import pandas as pd
from sqlmodel import ARRAY, BigInteger, Boolean, Date, DateTime, String

from .logging import get_logger
from .models import credit_schema, project_schema

logger = get_logger()


def update_file_status(file, session, status, error=None):
    logger.info(f'🔄 Updating file status: {file.url}')
    file.status = status
    file.error = error
    session.add(file)
    session.commit()
    session.refresh(file)
    logger.info(f'✅ File status updated: {file.url}')


def process_dataframe(df, table_name, engine, dtype_dict=None):
    logger.info(f'📝 Writing DataFrame to {table_name}')
    logger.info(f'engine: {engine}')
    df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_dict)
    logger.info(f'✅ Written 🧬 shape {df.shape} to {table_name}')


def process_files(*, engine, session, files: list):
    for file in files:
        try:
            if file.category == 'credits':
                logger.info(f'📚 Loading credit file: {file.url}')
                data = (
                    pd.read_parquet(file.url, engine='fastparquet')
                    .reset_index(drop=True)
                    .reset_index()
                    .rename(columns={'index': 'id'})
                )  # add id column
                df = credit_schema(data)
                credit_dtype_dict = {
                    'recorded_at': DateTime,
                    'project_id': String,
                    'quantity': BigInteger,
                    'vintage': BigInteger,
                    'transaction_date': Date,
                    'transaction_type': String,
                }
                process_dataframe(df, 'credit', engine, credit_dtype_dict)
                update_file_status(file, session, 'success')

            elif file.category == 'projects':
                logger.info(f'📚 Loading project file: {file.url}')
                data = pd.read_parquet(file.url, engine='fastparquet')
                df = project_schema(data)
                project_dtype_dict = {
                    'project_id': String,
                    'name': String,
                    'registry': String,
                    'proponent': String,
                    'protocol': ARRAY(String),
                    'category': ARRAY(String),
                    'status': String,
                    'country': String,
                    'listed_at': Date,
                    'is_compliance': Boolean,
                    'retired': BigInteger,
                    'issued': BigInteger,
                    'project_url': String,
                }
                process_dataframe(df, 'project', engine, project_dtype_dict)
                update_file_status(file, session, 'success')

            else:
                logger.info(f'❓ Unknown file category: {file.category}. Skipping file {file.url}')

        except Exception as e:
            trace = traceback.format_exc()
            logger.error(f'❌ Failed to process file: {file.url}')
            logger.error(trace)
            update_file_status(file, session, 'failure', error=str(e))
