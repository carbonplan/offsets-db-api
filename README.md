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

This backend service provides an FastAPI for accessing the CarbonPlan Offsets Database.
The database contains information about carbon offsets projects, credits.
It also contains information about the offset credits that have been issued for each project.

[![Fly.io Deployment](https://github.com/carbonplan/offsets-db-api/actions/workflows/fly.yml/badge.svg)](https://github.com/carbonplan/offsets-db-api/actions/workflows/fly.yml)
[![Database Update](https://github.com/carbonplan/offsets-db-api/actions/workflows/update-db.yaml/badge.svg)](https://github.com/carbonplan/offsets-db-api/actions/workflows/updated-db.yaml)
![MIT License](https://badgen.net/badge/license/MIT/blue)

| Project         | GitHub Repo                                                                                    |
| --------------- | ---------------------------------------------------------------------------------------------- |
| offsets-db-web  | [https://github.com/carbonplan/offsets-db-web](https://github.com/carbonplan/offsets-db-web)   |
| offsets-db-api  | [https://github.com/carbonplan/offsets-db-api](https://github.com/carbonplan/offsets-db-api)   |
| offsets-db-data | [https://github.com/carbonplan/offsets-db-data](https://github.com/carbonplan/offsets-db-data) |

## Installation

To install the package, you can use pip:

```console
python -m pip install git+https://github.com/carbonplan/offsets-db-api
```

You can also install the package locally by cloning the repository and running:

```console
git clone https://github.com/carbonplan/offsets-db-api
cd offsets-db-api
python -m pip install -e .
```

## Run locally

To run the API locally, you can use the following command:

```console
uvicorn offsets_db_api.main:app --reload
```

## license

All the code in this repository is [MIT](https://choosealicense.com/licenses/mit/) licensed. When possible, the data used by this project is licensed using the [CC-BY-4.0](https://choosealicense.com/licenses/cc-by-4.0/) license. We include attribution and additional license information for third party datasets, and we request that you also maintain that attribution if using this data.

## about us

CarbonPlan is a non-profit organization that uses data and science for climate action. We aim to improve the transparency and scientific integrity of carbon removal and climate solutions through open data and tools. Find out more at [carbonplan.org](https://carbonplan.org/) or get in touch by [opening an issue](https://github.com/carbonplan/offsets-db/issues/new) or [sending us an email](mailto:hello@carbonplan.org).
