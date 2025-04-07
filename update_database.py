import argparse
import datetime
import json
import os
import sys

import fsspec
import pandas as pd
import requests


def generate_path(*, date: datetime.date, bucket: str, category: str) -> str:
    """Generate S3 path for a given date and category."""
    return f'{bucket.rstrip("/")}/final/{date.strftime("%Y-%m-%d")}/{category}.parquet'


def calculate_date(*, days_back: int) -> datetime.date:
    """Calculate a date relative to today."""
    return datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=days_back)


def get_latest(*, bucket: str) -> list[dict[str, str]]:
    """Get the latest data files from the S3 bucket."""
    fs = fsspec.filesystem('s3')
    today, yesterday = calculate_date(days_back=0), calculate_date(days_back=1)

    items = [
        ('credits', 'credits-augmented', today, yesterday),
        ('projects', 'projects-augmented', today, yesterday),
        ('clips', 'curated-clips', today, yesterday),
        ('projecttypes', 'project-types', today, yesterday),
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
            print(f"Warning: Both {latest_path} and {previous_path} don't exist, skipping")
            continue

        data.append({'category': key, 'url': entry_url})

    # Handle weekly summaries
    weekly_summary_start = datetime.date(year=2024, month=2, day=6)
    weekly_summary_end = datetime.datetime.now(datetime.timezone.utc).date()
    date_ranges = pd.date_range(
        start=weekly_summary_start, end=weekly_summary_end, freq='W-TUE', inclusive='both'
    )

    added_weeks = set()
    for entry in date_ranges:
        value = entry.isocalendar()
        week_num = f'{value.year}-{value.week}'
        if week_num not in added_weeks:
            weekly_summary_path = generate_path(
                date=entry.date(), bucket=bucket, category='weekly-summary-clips'
            )
            if fs.exists(weekly_summary_path):
                data.append({'category': 'clips', 'url': weekly_summary_path})
                added_weeks.add(week_num)

    return data


def load_files_from_json(file_path: str) -> list[dict[str, str]]:
    """Load file definitions from a JSON file."""
    try:
        with open(file_path) as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f'Error loading file list: {e}')
        sys.exit(1)


def post_data_to_environment(
    *,
    env: str,
    url: str,
    files: list[dict[str, str]],
) -> None:
    """Post file definitions to the API."""
    # Get API key from environment
    if env == 'production':
        api_key = os.environ.get('OFFSETS_DB_API_KEY_PRODUCTION')
        if api_key is None:
            raise ValueError('OFFSETS_DB_API_KEY_PRODUCTION environment variable not set')
    else:
        api_key = os.environ.get('OFFSETS_DB_API_KEY_STAGING')
        if api_key is None:
            raise ValueError('OFFSETS_DB_API_KEY_STAGING environment variable not set')

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        'X-API-KEY': api_key,
    }

    print(f'\nSending {len(files)} files to {url}:')
    for file in files:
        print(f'- {file["category"]}: {file["url"]}')

    # Send the request
    response = requests.post(url, headers=headers, data=json.dumps(files))

    # Log the response
    if response.ok:
        print(f'\nSuccess in {env}:', response.json())
    else:
        print(f'\nFailed in {env}:', response.text)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Update offsets database with latest data files',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        'environment',
        choices=['staging', 'production'],
        help='Target environment for the update',
    )

    parser.add_argument(
        '--url',
        '-u',
        default='http://127.0.0.1:8000/files/',
        help='API endpoint URL',
    )

    parser.add_argument(
        '--bucket',
        '-b',
        default='s3://carbonplan-offsets-db',
        help='S3 bucket containing data files',
    )

    parser.add_argument(
        '--files',
        '-f',
        help='JSON file containing list of files to upload (overrides automatic discovery)',
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be uploaded without sending data',
    )

    args = parser.parse_args()

    print(f'Seeding {args.environment} database using URL: {args.url}')

    # Determine files to upload
    if args.files:
        files = load_files_from_json(args.files)
        print(f'Using {len(files)} files from {args.files}')
    else:
        files = get_latest(bucket=args.bucket)
        print(f'Found {len(files)} latest files from {args.bucket}')

    if not files:
        print('No files to upload!')
        sys.exit(1)

    if args.dry_run:
        print('\nDRY RUN - Would upload these files:')
        for file in files:
            print(f'- {file["category"]}: {file["url"]}')
        return

    post_data_to_environment(env=args.environment, url=args.url, files=files)


if __name__ == '__main__':
    main()
