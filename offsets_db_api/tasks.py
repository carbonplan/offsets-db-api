import csv
import datetime
import io
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
    values = []
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
        values.append(placeholder_project)
    if values:
        session.bulk_insert_mappings(Project, [p.model_dump() for p in values])

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
    chunk_size: int = 50000,
) -> None:
    logger.info(f'📝 Writing DataFrame to {table_name}')
    logger.info(f'engine: {engine}')

    # Convert Python lists to PostgreSQL arrays
    if dtype_dict:
        for col_name, dtype in dtype_dict.items():
            if 'ARRAY' in str(dtype) and col_name in df.columns:
                logger.info(f'Converting column {col_name} to PostgreSQL array format')
                df[col_name] = df[col_name].apply(
                    lambda x: '{' + ','.join(str(i) for i in x) + '}' if isinstance(x, list) else x
                )

    # Special high-performance path for large credit table in PostgreSQL
    if table_name == 'credit':
        with engine.begin() as conn:
            # Step 1: Ensure projects exist (needed for referential integrity)
            if table_name in {'credit', 'clipproject', 'projecttype'}:
                session = next(get_session())
                try:
                    logger.info(f'Processing data destined for {table_name} table...')
                    ensure_projects_exist(df, session)
                except IntegrityError:
                    logger.error(
                        '❌ Failed to ensure projects exist. Continuing with data insertion.'
                    )

            # Step 2: Get existing indexes and constraints to recreate later
            logger.info(f'Getting existing indexes and constraints for {table_name}')

            # Get indexes - exclude primary key indexes
            index_query = text("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = :table_name
                AND indexname NOT LIKE 'pk_%'
                AND indexname NOT LIKE '%_pkey'
            """)
            indexes = conn.execute(index_query, {'table_name': table_name}).fetchall()

            # Get foreign key constraints
            fk_query = text("""
                SELECT
                    tc.constraint_name,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                    JOIN information_schema.referential_constraints AS rc
                      ON rc.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = :table_name
            """)
            fk_constraints = conn.execute(fk_query, {'table_name': table_name}).fetchall()

            # Step 3: Drop constraints and indexes for better performance
            logger.info(f'Dropping foreign key constraints for {table_name}')
            for fk in fk_constraints:
                constraint_name = fk[0]
                conn.execute(
                    text(f'ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name}')
                )

            logger.info(f'Dropping indexes for {table_name}')
            for idx in indexes:
                index_name = idx[0]
                conn.execute(text(f'DROP INDEX IF EXISTS {index_name}'))

            # Step 4: Temporarily increase memory settings for better performance
            logger.info('Increasing PostgreSQL memory settings for better performance')

            current_maint_work_mem = conn.execute(text('SHOW maintenance_work_mem')).fetchone()[0]
            current_work_mem = conn.execute(text('SHOW work_mem')).fetchone()[0]

            conn.execute(text("SET maintenance_work_mem = '4GB'"))
            conn.execute(text("SET work_mem = '1GB'"))

            # Step 5: Drop and recreate table (avoids TRUNCATE which keeps indexes)
            logger.info(f'Dropping and recreating {table_name} table')
            if engine.dialect.has_table(conn, table_name):
                conn.execute(text(f'DROP TABLE IF EXISTS {table_name} CASCADE'))

            # Create empty table with correct schema
            empty_df = df.head(0)
            empty_df.to_sql(table_name, conn, if_exists='append', index=False, dtype=dtype_dict)

            # Step 6: Bulk load data with COPY
            logger.info(f'Bulk loading {len(df)} rows to {table_name} with COPY')

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_MINIMAL, header=False)
            csv_buffer.seek(0)

            dbapi_conn = conn.connection
            with dbapi_conn.cursor() as cur:
                columns = ', '.join([f'"{k}"' for k in df.columns])
                sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
                cur.copy_expert(sql=sql, file=csv_buffer)

            # Step 7: Recreate foreign key constraints
            logger.info(f'Recreating foreign key constraints for {table_name}')
            for fk in fk_constraints:
                constraint_name = fk[0]
                table_name = fk[1]
                column_name = fk[2]
                foreign_table = fk[3]
                foreign_column = fk[4]
                update_rule = fk[5]
                delete_rule = fk[6]

                sql = f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT {constraint_name}
                FOREIGN KEY ({column_name})
                REFERENCES {foreign_table}({foreign_column})
                ON UPDATE {update_rule} ON DELETE {delete_rule}
                """
                conn.execute(text(sql))

            # Step 8: Recreate indexes one by one in separate transactions
            logger.info(f'Recreating indexes for {table_name} in separate transactions')

            # Close current transaction first to split index creation
            conn.commit()

            for idx in indexes:
                index_name = idx[0]
                index_def = idx[1]

                logger.info(f'Creating index {index_name}')
                try:
                    # Open new connection/transaction for each index
                    with engine.begin() as index_conn:
                        index_conn.execute(text("SET maintenance_work_mem = '4GB'"))
                        index_conn.execute(text("SET work_mem = '1GB'"))
                        index_conn.execute(text(index_def))
                        logger.info(f'Successfully created index {index_name}')
                except Exception as e:
                    logger.warning(f'Failed to create index {index_name}: {str(e)}')
                    logger.warning('Continuing with remaining indexes...')

            # Step 9: Run ANALYZE for query optimization in a new transaction
            with engine.begin() as analyze_conn:
                logger.info(f'Running ANALYZE on {table_name}')
                analyze_conn.execute(text(f'ANALYZE {table_name}'))

            # Step 10: Reset memory settings to defaults in a final transaction
            with engine.begin() as reset_conn:
                logger.info('Resetting PostgreSQL memory settings to defaults')
                reset_conn.execute(text(f"SET maintenance_work_mem = '{current_maint_work_mem}'"))
                reset_conn.execute(text(f"SET work_mem = '{current_work_mem}'"))

            logger.info(f'✅ Optimized data loading completed for {table_name}')
    else:
        # Standard approach for other tables or non-PostgreSQL databases
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
                    logger.error(
                        '❌ Failed to ensure projects exist. Continuing with data insertion.'
                    )

            # Create table if it doesn't exist (with correct schema)
            if not engine.dialect.has_table(conn, table_name):
                # Create an empty DataFrame with same structure for table creation only
                empty_df = df.head(0)
                empty_df.to_sql(table_name, conn, if_exists='append', index=False, dtype=dtype_dict)

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_MINIMAL, header=False)
            csv_buffer.seek(0)

            # Get DBAPI connection
            dbapi_conn = conn.connection
            with dbapi_conn.cursor() as cur:
                columns = ', '.join([f'"{k}"' for k in df.columns])
                qualified_table_name = f'{table_name}'
                sql = f'COPY {qualified_table_name} ({columns}) FROM STDIN WITH CSV'
                cur.copy_expert(sql=sql, file=csv_buffer)

            logger.info(f'Loaded entire dataset to {table_name} using COPY')
            # Run ANALYZE for query optimization
            conn.execute(text(f'ANALYZE {table_name}'))
            # Commit the transaction
            conn.commit()

    logger.info(f'✅ Written 🧬 shape {df.shape} to {table_name}')


async def process_files(*, engine, session, files: list[File], chunk_size: int = 50000) -> None:
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

    success_files = [f for f in metrics['file_metrics'].values() if f.get('status') == 'success']
    failure_files = [f for f in metrics['file_metrics'].values() if f.get('status') == 'failure']
    success_count = sum(1 for _ in success_files)
    failure_count = sum(1 for _ in failure_files)
    logger.info('-' * 80)
    logger.info(f'✅ Successful files: {success_count}')
    logger.info(f'❌ Failed files: {failure_count}: {[f["url"] for f in failure_files]}')
    logger.info('=' * 80)
