<p align='left'>
  <a href='https://carbonplan.org/#gh-light-mode-only'>
    <img
      src='https://carbonplan-assets.s3.amazonaws.com/monogram/dark-small.png'
      height='48px'
    />
  </a>
  <a href='https://carbonplan.org/#gh-dark-mode-only'>
    <img
      src='https://carbonplan-assets.s3.amazonaws.com/monogram/light-small.png'
      height='48px'
    />
  </a>
</p>

# carbonplan / offsets-db-api

OffsetsDB-API, is a fastAPI application, designed to integrate and harmonize data related to the issuance and use of offset credits from the six major offset registries. This database aims to promote transparency and accountability in the carbon offsets market. It provides an accessible online tool, data downloads, and APIs for researchers, journalists, and regulators.

[![Fly.io Deployment](https://github.com/carbonplan/offsets-db-api/actions/workflows/fly.yml/badge.svg)](https://github.com/carbonplan/offsets-db-api/actions/workflows/fly.yml)
[![Database Update](https://github.com/carbonplan/offsets-db-api/actions/workflows/update-db.yaml/badge.svg)](https://github.com/carbonplan/offsets-db-api/actions/workflows/update-db.yaml)
![MIT License](https://badgen.net/badge/license/MIT/blue)
[![Code Coverage Status][codecov-badge]][codecov-link]

| Project         | GitHub Repo                                                                                    |
| --------------- | ---------------------------------------------------------------------------------------------- |
| offsets-db-web  | [https://github.com/carbonplan/offsets-db-web](https://github.com/carbonplan/offsets-db-web)   |
| offsets-db-api  | [https://github.com/carbonplan/offsets-db-api](https://github.com/carbonplan/offsets-db-api)   |
| offsets-db-data | [https://github.com/carbonplan/offsets-db-data](https://github.com/carbonplan/offsets-db-data) |

## installation

This project uses [pixi](https://pixi.sh) for dependency management. To get started:

```console
git clone https://github.com/carbonplan/offsets-db-api
cd offsets-db-api
pixi install
```

## local development

> [!NOTE]
> A running PostgreSQL instance is required. Two options are provided: Docker Compose (recommended) or a local server managed by pixi.

### Option A — Docker Compose (recommended)

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or any Docker engine).

```console
# 1. Copy the example env file and adjust if needed
cp .env.example .env

# 2. Start Postgres and apply migrations in one step
pixi run dev-setup

# 3. Start the API server with hot reload
pixi run serve
```

The API will be available at <http://localhost:8000> — interactive docs at <http://localhost:8000/docs>.

```console
# Run tests (DB must be running)
pixi run test

# Stop Postgres
pixi run db-down
```

### Option B — pixi-managed Postgres (no Docker)

The `postgresql` package is already in the pixi environment.

```console
# 1. Copy the example env file
cp .env.example .env

# 2. One-time: initialise the data directory and create the database
pixi run db-init

# 3. Start the server and apply migrations
pixi run db-start
pixi run migrate

# 4. Launch the API
pixi run serve
```

To stop the server: `pixi run db-stop`. The data directory is kept in `.pixi-postgres/` (gitignored).

## usage

```console
pixi run serve        # Start development server with hot reload
pixi run test         # Run tests (requires a running DB)
pixi run migrate      # Run database migrations
pixi run serve-prod   # Start production server
```

## license

All the code in this repository is [MIT](https://choosealicense.com/licenses/mit/) licensed.

> [!IMPORTANT]
> Data associated with this repository are subject to additional [terms of data access](https://github.com/carbonplan/offsets-db-data/blob/main/TERMS_OF_DATA_ACCESS).

## about us

CarbonPlan is a non-profit organization that uses data and science for climate action. We aim to improve the transparency and scientific integrity of carbon removal and climate solutions through open data and tools. Find out more at [carbonplan.org](https://carbonplan.org/) or get in touch by [opening an issue](https://github.com/carbonplan/offsets-db/issues/new) or [sending us an email](mailto:hello@carbonplan.org).

[codecov-badge]: https://img.shields.io/codecov/c/github/carbonplan/offsets-db-api.svg?logo=codecov
[codecov-link]: https://codecov.io/gh/carbonplan/offsets-db-api
