import json
import os
import sys
from datetime import datetime

import requests


def post_data_to_environment(env):
    # Determine the URL based on the environment
    url = os.environ.get(f'{env.upper()}_URL', f'https://offsets-db-{env}.fly.dev/files/')
    print(f'URL: {url}')

    # Set up the headers for the request
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    # Determine the date for the URL
    date_str = datetime.utcnow().strftime('%Y-%m-%d')

    # Set up the data for the request
    files = {
        'staging': [
            {
                'url': 's3://carbonplan-share/offsets-db-testing-data/final/credits-augmented.parquet',
                'category': 'credits',
            },
            {
                'url': 's3://carbonplan-share/offsets-db-testing-data/final/projects-augmented.parquet',
                'category': 'projects',
            },
            {
                'url': 's3://carbonplan-share/offsets-db-testing-data/final/clips.parquet',
                'category': 'clips',
            },
        ],
        'production': [
            {
                'url': f's3://carbonplan-offsets-db/final/{date_str}/credits-augmented.parquet',
                'category': 'credits',
            },
            {
                'url': f's3://carbonplan-offsets-db/final/{date_str}/projects-augmented.parquet',
                'category': 'projects',
            },
            {
                # 'url': f's3://carbonplan-offsets-db/final/{date_str}/clips.parquet',
                'url': 's3://carbonplan-share/offsets-db-testing-data/final/clips.parquet',
                'category': 'clips',
            },
        ],
    }

    # Send the request
    response = requests.post(url, headers=headers, data=json.dumps(files[env]))

    # Log the response
    if response.ok:
        print(f'Success in {env}:', response.json())
    else:
        print(f'Failed in {env}:', response.text)


if __name__ == '__main__':
    env = sys.argv[1] if len(sys.argv) > 1 else 'staging'
    print(f'Seeding {env} database...')
    post_data_to_environment(env)
    print('Done.')
