import datetime
import time
import traceback
from collections import defaultdict

import pandas as pd
from offsets_db_data.models import clip_schema, credit_schema, project_schema
from offsets_db_data.registry import get_registry_from_project_id
from sqlalchemy.exc import IntegrityError
from sqlmodel import ARRAY, BigInteger, Boolean, Date, DateTime, Session, String, col, select, text

from offsets_db_api.cache import watch_dog_file
from offsets_db_api.database import get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import File, Project

logger = get_logger()


def update_file_status(file: File, session, status: str, error: str | None = None) -> None:
    logger.info(f'🔄 Updating file status: {file.url}')
    file.status = status
    file.error = error
    file.recorded_at = datetime.datetime.now(datetime.timezone.utc)
    session.add(file)
    session.commit()
    session.refresh(file)
    logger.info(f'✅ File status updated: {file.url}')


def ensure_projects_exist(df: pd.DataFrame, session: Session) -> None:
    """
    Ensure all project IDs in the dataframe exist in the database.
    If not, create placeholder projects for missing IDs.
    """
    logger.info('🔍 Checking for missing project IDs')

    # Get all unique project IDs from the dataframe
    credit_project_ids = df['project_id'].unique()

    # Query existing project IDs
    existing_project_ids = set(
        session.exec(
            select(Project.project_id).where(col(Project.project_id).in_(credit_project_ids))
        ).all()
    )

    # Identify missing project IDs
    missing_project_ids = set(credit_project_ids) - existing_project_ids

    logger.info(f'🔍 Found {len(existing_project_ids)} existing project IDs')
    logger.info(f'🔍 Found {len(missing_project_ids)} missing project IDs: {missing_project_ids}')

    # Create placeholder projects for missing IDs
    urls = {
        'verra': 'https://registry.verra.org/app/projectDetail/VCS/',
        'gold-standard': 'https://registry.goldstandard.org/projects?q=gs',
        'american-carbon-registry': 'https://acr2.apx.com/mymodule/reg/prjView.asp?id1=',
        'climate-action-reserve': 'https://thereserve2.apx.com/mymodule/reg/prjView.asp?id1=',
        'art-trees': 'https://art.apx.com/mymodule/reg/prjView.asp?id1=',
    }
    for project_id in missing_project_ids:
        registry = get_registry_from_project_id(project_id)
        if url := urls.get(registry):
            url = f'{url}{project_id[3:]}'
        placeholder_project = Project(
            project_id=project_id,
            registry=registry,
            category='unknown',
            protocol=['unknown'],
            project_url=url,
            type='unknown',
            type_source='carbonplan',
        )
        session.add(placeholder_project)

    try:
        session.commit()
        logger.info(f'✅ Added {len(missing_project_ids)} missing project IDs to the database')
    except IntegrityError as exc:
        session.rollback()
        logger.error(f'❌ Error creating placeholder projects: {exc}')
        raise


def process_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine,
    dtype_dict: dict | None = None,
    chunk_size: int = 8000,
) -> None:
    logger.info(f'📝 Writing DataFrame to {table_name}')
    logger.info(f'engine: {engine}')

    # Define PostgreSQL COPY method for faster inserts
    def psql_insert_copy(table, conn, keys, data_iter):
        """Execute SQL statement inserting data using PostgreSQL COPY"""
        import csv
        from io import StringIO

        # Get a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join([f'"{k}"' for k in keys])
            if table.schema:
                table_name = f'{table.schema}.{table.name}'
            else:
                table_name = table.name

            sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
            cur.copy_expert(sql=sql, file=s_buf)

    with engine.begin() as conn:
        if engine.dialect.has_table(conn, table_name):
            # Instead of dropping table (which results in data type, schema overrides), delete all rows.
            conn.execute(text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;'))

        if table_name in {'credit', 'clipproject', 'projecttype'}:
            session = next(get_session())
            try:
                logger.info(f'Processing data destined for {table_name} table...')
                ensure_projects_exist(df, session)
            except IntegrityError:
                logger.error('❌ Failed to ensure projects exist. Continuing with data insertion.')

        # Check if we're using PostgreSQL to enable COPY method
        is_postgresql = 'postgresql' in engine.dialect.name.lower()
        insert_method = psql_insert_copy if is_postgresql else 'multi'

        # Still process in chunks to avoid memory issues
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            chunk.to_sql(
                table_name,
                conn,
                if_exists='append',
                index=False,
                dtype=dtype_dict,
                method=insert_method,
            )
            logger.info(
                f'Processed chunk {i // chunk_size + 1}/{(len(df) - 1) // chunk_size + 1} of {table_name}'
            )

    logger.info(f'✅ Written 🧬 shape {df.shape} to {table_name}')


async def process_files(*, engine, session, files: list[File], chunk_size: int = 10000) -> None:
    metrics = {
        'total_start_time': time.time(),
        'file_metrics': defaultdict(dict),
        'category_metrics': defaultdict(list),
    }
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
        file_start_time = time.time()
        metrics['file_metrics'][file.id]['start_time'] = file_start_time
        metrics['file_metrics'][file.id]['url'] = file.url
        metrics['file_metrics'][file.id]['category'] = file.category
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
                    'retirement_account': String,
                    'retirement_reason': String,
                    'retirement_note': String,
                    'retirement_beneficiary': String,
                    'retirement_beneficiary_harmonized': String,
                }
                process_dataframe(df, 'credit', engine, credit_dtype_dict, chunk_size=chunk_size)
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
                    'category': String,
                    'type': String,
                    'type_source': String,
                    'status': String,
                    'country': String,
                    'listed_at': Date,
                    'is_compliance': Boolean,
                    'retired': BigInteger,
                    'issued': BigInteger,
                    'project_url': String,
                }

                process_dataframe(df, 'project', engine, project_dtype_dict, chunk_size=chunk_size)
                update_file_status(file, session, 'success')

            else:
                logger.info(f'❓ Unknown file category: {file.category}. Skipping file {file.url}')

            metrics['file_metrics'][file.id]['status'] = 'success'

        except Exception as e:
            trace = traceback.format_exc()
            logger.error(f'❌ Failed to process file: {file.url}')
            logger.error(trace)
            update_file_status(file, session, 'failure', error=str(e))
            metrics['file_metrics'][file.id]['status'] = 'failure'
            metrics['file_metrics'][file.id]['error'] = str(e)

        file_end_time = time.time()
        processing_time = file_end_time - file_start_time
        metrics['file_metrics'][file.id]['processing_time'] = processing_time
        metrics['category_metrics'][file.category].append(processing_time)
        logger.info(f'⏱️ File {file.url} processed in {processing_time:.2f} seconds')

    if not clips_files:
        logger.info('No clip files to process')
        return

    clips_dfs = []
    for file in clips_files:
        file_start_time = time.time()
        metrics['file_metrics'][file.id]['start_time'] = file_start_time
        metrics['file_metrics'][file.id]['url'] = file.url
        metrics['file_metrics'][file.id]['category'] = file.category
        try:
            logger.info(f'📚 Loading clip file: {file.url}')
            data = pd.read_parquet(file.url, engine='fastparquet')
            clips_dfs.append(data)
            update_file_status(file, session, 'success')
            metrics['file_metrics'][file.id]['status'] = 'success'

        except Exception as e:
            trace = traceback.format_exc()
            logger.error(f'❌ Failed to process file: {file.url}')
            logger.error(trace)
            update_file_status(file, session, 'failure', error=str(e))
            metrics['file_metrics'][file.id]['status'] = 'failure'
            metrics['file_metrics'][file.id]['error'] = str(e)

        file_end_time = time.time()
        processing_time = file_end_time - file_start_time
        metrics['file_metrics'][file.id]['processing_time'] = processing_time
        metrics['category_metrics']['clips'].append(processing_time)
        logger.info(f'⏱️ Clip file {file.url} processed in {processing_time:.2f} seconds')

    with engine.begin() as conn:
        conn.execute(text('TRUNCATE TABLE clipproject, clip RESTART IDENTITY CASCADE;'))

    df = pd.concat(clips_dfs).reset_index(drop=True).reset_index().rename(columns={'index': 'id'})
    df = clip_schema.validate(df)

    clips_df = df.drop(columns=['project_ids'])
    clip_dtype_dict = {'tags': ARRAY(String)}
    process_dataframe(clips_df, 'clip', engine, clip_dtype_dict, chunk_size=chunk_size)

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

    process_dataframe(clip_projects_df, 'clipproject', engine, chunk_size=chunk_size)

    # modify the watch_dog_file
    with open(watch_dog_file, 'w') as f:
        now = datetime.datetime.now(datetime.timezone.utc)
        logger.info(f'✅ Updated watch_dog_file: {watch_dog_file} to {now}')
        f.write(now.strftime('%Y-%m-%d %H:%M:%S'))

        # Calculate total processing time
    total_time = time.time() - metrics['total_start_time']
    metrics['total_time'] = total_time

    # Log performance metrics summary
    logger.info('=' * 80)
    logger.info('📊 PERFORMANCE METRICS SUMMARY')
    logger.info(f'⏱️ Total processing time: {total_time:.2f} seconds')
    logger.info(f'📁 Total files processed: {len(files)}')

    # Summary by category
    logger.info('-' * 80)
    logger.info('📊 CATEGORY SUMMARY:')
    for category, times in metrics['category_metrics'].items():
        if times:
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            logger.info(
                f'  - {category}: {len(times)} files, avg: {avg_time:.2f}s, min: {min_time:.2f}s, max: {max_time:.2f}s'
            )

    # Summary of successful vs failed files
    success_count = sum(1 for f in metrics['file_metrics'].values() if f.get('status') == 'success')
    failure_count = sum(1 for f in metrics['file_metrics'].values() if f.get('status') == 'failure')
    logger.info('-' * 80)
    logger.info(f'✅ Successful files: {success_count}')
    logger.info(f'❌ Failed files: {failure_count}')
    logger.info('=' * 80)
