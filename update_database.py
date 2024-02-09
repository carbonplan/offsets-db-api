import datetime
import json
import os
import sys

import fsspec
import pandas as pd
import requests


def generate_path(*, date: datetime.date, bucket: str, category: str) -> str:
    return f"{bucket.rstrip('/')}/final/{date.strftime('%Y-%m-%d')}/{category}.parquet"


def calculate_date(*, days_back: int) -> datetime.date:
    return datetime.datetime.utcnow().date() - datetime.timedelta(days=days_back)


def get_latest(*, bucket: str):
    fs = fsspec.filesystem('s3')  # Assuming S3, adjust accordingly
    today, yesterday = calculate_date(days_back=0), calculate_date(days_back=1)

    items = [
        ('credits', 'credits-augmented', today, yesterday),
        ('projects', 'projects-augmented', today, yesterday),
        ('clips', 'curated-clips', today, yesterday),
    ]

    data = []
    for key, category, latest_date, previous_date in items:
        latest_path = generate_path(date=latest_date, bucket=bucket, category=category)
        previous_path = generate_path(date=previous_date, bucket=bucket, category=category)

        if fs.exists(latest_path):
            entry_url = latest_path
        elif fs.exists(previous_path):
            entry_url = previous_path
        else:
            raise ValueError(f"both {latest_path} and {previous_path} file paths don't exist")

        data.append({'category': key, 'url': entry_url})

    weekly_summary_start = datetime.date(year=2024, month=2, day=6)
    weekly_summary_end = datetime.datetime.utcnow().date()
    date_ranges = pd.date_range(
        start=weekly_summary_start, end=weekly_summary_end, freq='W-TUE', inclusive='both'
    )

    added_weeks = set()

    for entry in date_ranges:
        week_num = entry.isocalendar()[1]
        if week_num not in added_weeks:
            weekly_summary_path = generate_path(
                date=entry.date(), bucket=bucket, category='weekly-summary-clips'
            )
            if fs.exists(weekly_summary_path):
                data.append({'category': 'clips', 'url': weekly_summary_path})
                added_weeks.add(week_num)

    return data


def post_data_to_environment(*, env: str, bucket: str) -> None:
    # Set up the headers for the request
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    if env == 'production':
        files = get_latest(bucket=bucket)

    else:
        files = [
            {
                'url': 's3://carbonplan-share/offsets-db-testing-data/final/credits-augmented.parquet',
                'category': 'credits',
            },
            {
                'url': 's3://carbonplan-share/offsets-db-testing-data/final/projects-augmented.parquet',
                'category': 'projects',
            },
            {
                'url': 's3://carbonplan-offsets-db/final/2024-02-08/curated-clips.parquet',
                'category': 'clips',
            },
            {
                'url': 's3://carbonplan-offsets-db/final/2024-02-13/weekly-summary-clips.parquet',
                'category': 'clips',
            },
        ]

    [print(file) for file in files]

    # get X-API-KEY from env and use it in headers
    if env == 'production':
        api_key = os.environ.get('OFFSETS_DB_API_KEY_PRODUCTION')
        if api_key is None:
            raise ValueError('OFFSETS_DB_API_KEY_PRODUCTION environment variable not set')

    else:
        api_key = os.environ.get('OFFSETS_DB_API_KEY_STAGING')
        if api_key is None:
            raise ValueError('OFFSETS_DB_API_KEY_STAGING environment variable not set')

    headers['X-API-KEY'] = api_key

    # Send the request
    response = requests.post(url, headers=headers, data=json.dumps(files))

    # Log the response
    if response.ok:
        print(f'Success in {env}:', response.json())
    else:
        print(f'Failed in {env}:', response.text)


if __name__ == '__main__':
    env = sys.argv[1] if len(sys.argv) > 1 else 'staging'
    if env not in ['staging', 'production']:
        raise ValueError(f'env must be either "staging" or "production", not {env}')
    url = sys.argv[2] if len(sys.argv) > 2 else 'http://127.0.0.1:8000/files/'
    bucket = 's3://carbonplan-offsets-db'
    print(f'Seeding {env} database using URL: {url}...')
    post_data_to_environment(env=env, bucket=bucket)
