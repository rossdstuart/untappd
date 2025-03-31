import requests
import time
import json
import boto3
import os
import pandas as pd
import io

def get_secret():
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId='untappd_api')
    secret = json.loads(response['SecretString'])
    return secret['CLIENT_ID'], secret['CLIENT_SECRET']

brewery_id = 23570 #hop butcher brewery ID

def get_brewery_beers(get_brewery_id):
    """
    Retrieves a list of beers from a specific brewery using the Untappd API.

    Parameters:
    brewery_id (int): The ID of the brewery to retrieve beers from.

    Returns:
    list: A list of dictionaries, where each dictionary represents a beer.
    Each beer dictionary contains information such as beer name, ID, style, ABV, IBU, description, and Untappd URL.
    """

    client_id, client_secret = get_secret()

    base_url = f"https://api.untappd.com/v4/brewery/beer_list/{get_brewery_id}"
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "offset": 0,
        "limit": 50  # Max allowed per API docs
    }

    all_beers = []

    while True:
        response = requests.get(
            base_url,
            params=params,
            verify = False,
            headers={"User-Agent": "HopButcherBeerList/1.0"}
        )

        # Handle rate limiting
        if response.status_code == 429:
            reset_time = int(response.headers.get('X-Ratelimit-Reset', 60))
            print(f"Rate limited. Waiting {reset_time} seconds")
            time.sleep(reset_time)
            continue

        response.raise_for_status()
        data = response.json()

        if 'beers' in data['response']:
            all_beers.extend(data['response']['beers']['items'])

            # Check if we've retrieved all available beers
            if len(data['response']['beers']['items']) < params['limit']:
                break

            params['offset'] += params['limit']
        else:
            break

        # Respect rate limits
        remaining = int(response.headers.get('X-Ratelimit-Remaining', 100))
        if remaining < 5:
            time.sleep(1)

        # Convert the list of dictionaries to a DataFrame
        # all_beers_raw = pd.DataFrame(all_beers)
        beers_int = pd.read_json(io.StringIO(all_beers), orient='records')
        return beers_df['beer']

try:
    beers_df = get_brewery_beers(brewery_id)
    print(beers_df)

    # for idx, beer in enumerate(beers, 1):
    #     beer_info = beer['beer']
    #     print(f"{beer_info['bid']} - {beer_info['beer_name']}")

    # Print the beer information using DataFrame columns
    # print(beers_df[['beer.bid', 'beer.beer_name']])

    # Define the S3 bucket name and file path
    bucket_name = os.environ['BUCKET_NAME']
    local_file_path = f"/tmp/{brewery_id}.txt"
    s3_file_path = f"beer_list/{brewery_id}.txt"

    # Write the beer information to the S3 file
    # We'll always write to local first, and the file shouldn't exist prior.
    beers_df.to_csv(local_file_path, index=False)
    # beers_df[['beer.bid', 'beer.beer_name']].to_csv(local_file_path, index=False, header=False, sep=' ')

    # Upload the file
    s3_client = boto3.client('s3')
    ##### s3_client.upload_file(local_file_path, bucket_name, s3_file_path)

    # Write the beer information to the S3 file
    # We'll always write to local first, and the file shouldn't exist prior.
    # with open(local_file_path, 'w') as file:
    #     for beer in beers:
    #         beer_info = beer['beer']
    #         file.write(f"{beer_info['bid']} {beer_info['beer_name']}\n")

    # compare the local file with the S3 file, if S3 file doesn't exist write it, and compare on next run.


    # # Upload the file
    # s3_client = boto3.client('s3')
    # s3_client.upload_file(local_file_path, bucket_name, s3_file_path)


except requests.exceptions.HTTPError as e:
    print(f"API Error: {e.response.text}")
except Exception as e:
    print(f"Error: {str(e)}")
