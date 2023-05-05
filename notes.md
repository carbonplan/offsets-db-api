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
from carbonplan_offsets_db.models import Project


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
