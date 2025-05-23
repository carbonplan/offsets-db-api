# Notes

## Database Setup

To set up the database, first create a database in your local postgres server. To use a local postgres server:

- Install <https://postgresapp.com/>
- Start the server using the postgres app
- Run `echo "CREATE DATABASE ${Your Database Name};" | psql` to create a database

Then, create a `.env` file in the root directory of the repository with the following content:

```bash
OFFSETS_DATABASE_URL=postgresql://localhost/${Your Database Name}
```

Or if you want to use an environment variable, you can set the `OFFSETS_DATABASE_URL` environment variable to the database url:

```bash
export OFFSETS_DATABASE_URL=postgresql://localhost/${Your Database Name}
```

## Database Migration with Alembic

To generate alembic migrations, first run the following init command:

```bash
python -m alembic init migrations
```

Then, edit the `alembic.ini` file to point to the correct database.

After that, update the `migrations/env.py` file to point to the correct database and importing all models we want alembic to migrate. This consists of adding the following content to the file:

```python
import os
from sqlmodel import SQLModel
from offsets_db_api.models import Project


# https://stackoverflow.com/questions/37890284/ini-file-load-environment-variable
database_url = os.environ["OFFSETS_DATABASE_URL"]
if database_url.startswith("postgres://"):
    # Fix Heroku's incompatible postgres database uri
    # https://stackoverflow.com/a/67754795/3266235
    database_url = database_url.replace("postgres://", "postgresql://", 1)

config.set_main_option("sqlalchemy.url", database_url)

target_metadata = SQLModel.metadata

```

### Resetting the Database / migrations

```python
python -m alembic downgrade base
```

source: [stackoverflow](https://stackoverflow.com/questions/30507853/how-to-clear-history-and-run-all-migrations-from-the-beginning)

### Deleting all tables in the database

```shell
fly pg connect --app offsets-db-postgres-staging
```

```sql
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE;';
    END LOOP;
END $$;
```

or connect to a different database and drop the database

```bash
❯ fly pg connect --app offsets-db-postgres-staging --database repmgr
```

```sql
repmgr=# SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'postgres'
  AND pid <> pg_backend_pid();
 pg_terminate_backend
----------------------
 t
 t
 t
 t
(4 rows)

repmgr=# DROP DATABASE postgres;
DROP DATABASE
repmgr=# CREATE DATABASE postgres;
CREATE DATABASE
repmgr=#
```

## Recreating the postgres database on fly

To recreate the postgres database on fly, you can use the following commands:

```bash
fly pg create --name offsets-db-postgres --region dfw --vm-size shared-cpu-4x
```

once the database is created, you can then set the database url

```bash
fly secrets set OFFSETS_DATABASE_URL=postgres://<username>:<password>@<host>:<port>/<database> --config fly.prod.toml
```
