import datetime
import json
import sys

import fsspec
import requests


def generate_path(*, date: datetime.date, bucket: str, category: str) -> str:
    return f"{bucket.rstrip('/')}/final/{date.strftime('%Y-%m-%d')}/{category}.parquet"


def calculate_date(*, days_back: int) -> datetime.date:
    return datetime.datetime.utcnow().date() - datetime.timedelta(days=days_back)


def get_latest(*, bucket: str):
    fs = fsspec.filesystem('s3')  # Assuming S3, adjust accordingly
    today, yesterday = calculate_date(days_back=0), calculate_date(days_back=1)
    this_week_monday, last_week_monday = (
        calculate_date(days_back=today.weekday()),
        calculate_date(days_back=today.weekday() + 7),
    )

    items = [
        ('credits', 'credits-augmented', today, yesterday),
        ('projects', 'projects-augmented', today, yesterday),
        ('clips', 'curated-clips', today, yesterday),
        ('clips', 'weekly-summary-clips', this_week_monday, last_week_monday),
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
                'url': 's3://carbonplan-offsets-db/final/2023-12-08/curated-clips.parquet',
                'category': 'clips',
            },
            {
                'url': 's3://carbonplan-offsets-db/final/2023-12-04/weekly-summary-clips.parquet',
                'category': 'clips',
            },
        ]

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
    url = sys.argv[2] if len(sys.argv) > 2 else 'http://127.0.0.1:8000//files/'
    bucket = 's3://carbonplan-offsets-db'
    print(f'Seeding {env} database using URL: {url}...')
    post_data_to_environment(env=env, bucket=bucket)
