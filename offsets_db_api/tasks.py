import traceback

import pandas as pd
from offsets_db_data.models import clip_schema, credit_schema, project_schema
from sqlmodel import ARRAY, BigInteger, Boolean, Date, DateTime, String, text

from .logging import get_logger
from .models import File

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


def process_files(*, engine, session, files: list[File]):
    # loop over files and make sure projects are first in the list to ensure the delete cascade works
    ordered_files: list[File] = []
    for file in files:
        if file.category == 'projects':
            ordered_files.insert(0, file)
        else:
            ordered_files.append(file)

    clips_files = [file for file in ordered_files if file.category == 'clips']
    other_files = [file for file in ordered_files if file.category != 'clips']

    logger.info(f'📚 Loading files: {ordered_files}')

    for file in other_files:
        try:
            if file.category == 'credits':
                logger.info(f'📚 Loading credit file: {file.url}')
                data = (
                    pd.read_parquet(file.url, engine='fastparquet')
                    .reset_index(drop=True)
                    .reset_index()
                    .rename(columns={'index': 'id'})
                )  # add id column
                df = credit_schema.validate(data)
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
                df = project_schema.validate(data)
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

                # Execute raw SQL to drop dependent tables and the project table
                with engine.begin() as conn:
                    # conn.execute(text("DROP TABLE IF EXISTS clip, credit, project;"))
                    conn.execute(text('DROP TABLE IF EXISTS project CASCADE;'))

                process_dataframe(df, 'project', engine, project_dtype_dict)
                update_file_status(file, session, 'success')

            else:
                logger.info(f'❓ Unknown file category: {file.category}. Skipping file {file.url}')

        except Exception as e:
            trace = traceback.format_exc()
            logger.error(f'❌ Failed to process file: {file.url}')
            logger.error(trace)
            update_file_status(file, session, 'failure', error=str(e))

    clips_dfs = []
    for file in clips_files:
        try:
            logger.info(f'📚 Loading clip file: {file.url}')
            data = pd.read_parquet(file.url, engine='fastparquet')
            clips_dfs.append(data)
            update_file_status(file, session, 'success')

        except Exception as e:
            trace = traceback.format_exc()
            logger.error(f'❌ Failed to process file: {file.url}')
            logger.error(trace)
            update_file_status(file, session, 'failure', error=str(e))

    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS clipproject, clip CASCADE;'))

    df = pd.concat(clips_dfs).reset_index(drop=True).reset_index().rename(columns={'index': 'id'})
    df = clip_schema.validate(df)

    clips_df = df.drop(columns=['project_ids'])
    clip_dtype_dict = {'tags': ARRAY(String)}
    process_dataframe(clips_df, 'clip', engine, clip_dtype_dict)

    # Prepare ClipProject data
    clip_projects_data = []
    index = 0
    for _, row in df.iterrows():
        clip_id = row['id']  # Assuming 'id' is the primary key in Clip model
        project_ids = row['project_ids']
        for project_id in project_ids:
            clip_projects_data.append({'id': index, 'clip_id': clip_id, 'project_id': project_id})
            index += 1

    # Convert to DataFrame
    clip_projects_df = pd.DataFrame(clip_projects_data)

    process_dataframe(clip_projects_df, 'clipproject', engine)
