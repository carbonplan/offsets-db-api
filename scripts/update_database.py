import argparse
import datetime
import json
import os
import sys
import time

import fsspec
import httpx
import pandas as pd


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
            raise ValueError(f"both {latest_path} and {previous_path} file paths don't exist")

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


def _get_api_key(env: str) -> str:
    var = 'OFFSETS_DB_API_KEY_PRODUCTION' if env == 'production' else 'OFFSETS_DB_API_KEY_STAGING'
    key = os.environ.get(var)
    if key is None:
        raise ValueError(f'{var} environment variable not set')
    return key


def _request(method: str, url: str, headers: dict, timeout: float = 30, **kwargs) -> httpx.Response:
    try:
        return httpx.request(method, url, headers=headers, timeout=timeout, **kwargs)
    except httpx.ConnectError:
        print(f'Error: could not connect to {url}')
        print('Is the API server running?')
        sys.exit(1)
    except httpx.TimeoutException:
        print(f'Error: request to {url} timed out after {timeout:.0f}s')
        sys.exit(1)


def _poll_until_complete(
    *,
    base_url: str,
    file_ids: list[int],
    headers: dict,
    initial_delay: float = 2.0,
    max_delay: float = 30.0,
    timeout: float = 600.0,
) -> list[dict]:
    """Poll file statuses with exponential backoff until all leave pending state."""
    pending = set(file_ids)
    results: dict[int, dict] = {}
    deadline = time.monotonic() + timeout
    delay = initial_delay

    print(f'\nPolling status for {len(file_ids)} file(s)...')

    while pending:
        if time.monotonic() > deadline:
            timed_out = [str(i) for i in pending]
            print(f'Timed out waiting for file(s): {", ".join(timed_out)}')
            sys.exit(1)

        time.sleep(delay)
        delay = min(delay * 2, max_delay)

        for file_id in list(pending):
            resp = _request('GET', f'{base_url.rstrip("/")}/{file_id}', headers=headers)
            if not resp.is_success:
                print(f'  [{file_id}] HTTP {resp.status_code} polling status — skipping')
                continue
            file = resp.json()
            if file['status'] != 'pending':
                pending.discard(file_id)
                results[file_id] = file
                icon = '✓' if file['status'] == 'success' else '✗'
                error = f'  error: {file["error"]}' if file.get('error') else ''
                print(
                    f'  {icon} [{file_id}] {file["category"]:8s} {file["status"]:8s}  {file["url"]}{error}'
                )

    return list(results.values())


def post_data_to_environment(
    *,
    env: str,
    url: str,
    files: list[dict[str, str]],
    post_timeout: float = 120,
) -> None:
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        'X-API-KEY': _get_api_key(env),
    }

    print(f'\nSending {len(files)} file(s) to {url}:')
    for file in files:
        print(f'- {file["category"]}: {file["url"]}')

    response = _request('POST', url, headers=headers, json=files, timeout=post_timeout)
    if not response.is_success:
        print(f'\nFailed in {env}: HTTP {response.status_code} {response.reason_phrase}')
        if body := response.text.strip():
            print(body)
        sys.exit(1)

    queued = response.json()
    file_ids = [f['id'] for f in queued]
    print(f'Queued {len(file_ids)} file(s) with ids: {file_ids}')

    results = _poll_until_complete(base_url=url, file_ids=file_ids, headers=headers)

    failures = [f for f in results if f['status'] == 'failure']
    if failures:
        print(f'\n{len(failures)} file(s) failed in {env}:')
        for f in failures:
            print(f'  - [{f["id"]}] {f["url"]}')
            if f.get('error'):
                print(f'    {f["error"]}')
        sys.exit(1)

    print(f'\nAll {len(results)} file(s) processed successfully in {env}.')


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
