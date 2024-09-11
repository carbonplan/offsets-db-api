import datetime
import traceback

import pandas as pd
from offsets_db_data.models import clip_schema, credit_schema, project_schema
from offsets_db_data.registry import get_registry_from_project_id
from sqlalchemy.exc import IntegrityError
from sqlmodel import ARRAY, BigInteger, Boolean, Date, DateTime, String, select, text

from offsets_db_api.cache import watch_dog_file
from offsets_db_api.database import get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import File, Project

logger = get_logger()


def update_file_status(file, session, status, error=None):
    logger.info(f'🔄 Updating file status: {file.url}')
    file.status = status
    file.error = error
    file.recorded_at = datetime.datetime.now(datetime.timezone.utc)
    session.add(file)
    session.commit()
    session.refresh(file)
    logger.info(f'✅ File status updated: {file.url}')


def process_dataframe(df, table_name, engine, dtype_dict=None):
    logger.info(f'📝 Writing DataFrame to {table_name}')
    logger.info(f'engine: {engine}')

    with engine.begin() as conn:
        if engine.dialect.has_table(conn, table_name):
            # Instead of dropping table (which results in data type, schema overrides), delete all rows.
            conn.execute(text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;'))

        if table_name in {'credit', 'clipproject'}:
            session = next(get_session())
            try:
                # Get all unique project IDs from the credits dataframe
                credit_project_ids = df['project_id'].unique()

                # Query existing project IDs
                existing_project_ids = set(
                    session.exec(
                        select(Project.project_id).where(Project.project_id.in_(credit_project_ids))
                    ).all()
                )

                # Identify missing project IDs
                missing_project_ids = set(credit_project_ids) - existing_project_ids

                logger.info(f'🔍 Found {len(existing_project_ids)} existing project IDs')
                logger.info(
                    f'🔍 Found {len(missing_project_ids)} missing project IDs: {missing_project_ids}'
                )

                for project_id in missing_project_ids:
                    placeholder_project = Project(
                        project_id=project_id, registry=get_registry_from_project_id(project_id)
                    )
                    session.add(placeholder_project)

                session.commit()
                logger.info(
                    f'✅ Added {len(missing_project_ids)} missing project IDs: {missing_project_ids} to the database'
                )

            except IntegrityError as exc:
                session.rollback()
                logger.error(f'❌ Error creating placeholder projects: {exc}')

        # write the data
        df.to_sql(table_name, conn, if_exists='append', index=False, dtype=dtype_dict)

    logger.info(f'✅ Written 🧬 shape {df.shape} to {table_name}')


async def process_files(*, engine, session, files: list[File]):
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

                process_dataframe(df, 'project', engine, project_dtype_dict)
                update_file_status(file, session, 'success')

            else:
                logger.info(f'❓ Unknown file category: {file.category}. Skipping file {file.url}')

        except Exception as e:
            trace = traceback.format_exc()
            logger.error(f'❌ Failed to process file: {file.url}')
            logger.error(trace)
            update_file_status(file, session, 'failure', error=str(e))

    if not clips_files:
        logger.info('No clip files to process')
        return

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
        conn.execute(text('TRUNCATE TABLE clipproject, clip RESTART IDENTITY CASCADE;'))

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

    # modify the watch_dog_file
    with open(watch_dog_file, 'w') as f:
        now = datetime.datetime.now(datetime.timezone.utc)
        logger.info(f'✅ Updated watch_dog_file: {watch_dog_file} to {now}')
        f.write(now.strftime('%Y-%m-%d %H:%M:%S'))
