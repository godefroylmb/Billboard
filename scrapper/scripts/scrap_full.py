import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import NoCredentialsError
import argparse
import configparser
from utils import generate_dates

minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')
bucket_name = 'billboard'

def process_date(session, date_str, bucket_name, url, object_key):
    print(date_str)
    year, month, day = map(int, date_str.split('-'))
    full_url = f'{url}{date_str}'
    r = session.get(full_url)
    soup = BeautifulSoup(r.content, 'html.parser')
    result = soup.select('.o-chart-results-list-row-container')

    csv_content = []

    for res in result:
        ranks = []
        song_name = res.find('h3').text.strip().replace(',', ';')
        artist = res.find('h3').find_next('span').text.strip().replace(',', '|')
        rank = res.find('span').text.strip()
        numbers = res.select('.c-label')[5:]
        ranks.extend(number.text.strip() for number in numbers)

        img_tag = res.select_one('.c-lazy-image__img')
        img_url = img_tag['data-lazy-src'] if img_tag and 'data-lazy-src' in img_tag.attrs else "#"
        img_url = "#" if img_url == "https://www.billboard.com/wp-content/themes/vip/pmc-billboard-2021/assets/public/lazyload-fallback.gif" else img_url

        csv_content.append([date_str, song_name, artist, rank, ranks[0], ranks[1], ranks[2], img_url])

    return csv_content

def upload_to_minio(csv_content_str, bucket_name, object_key, s3_client):
    try:
        s3_client.put_object(Body=csv_content_str, Bucket=bucket_name, Key=object_key)
        print(f"Content uploaded to {bucket_name}/{object_key}")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"Failed to upload content to {bucket_name}/{object_key}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Billboard Scraper')
    parser.add_argument('--config', default='config.ini', help='Path to the configuration file')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    url = config.get('main', 'url')
    object_key = config.get('main', 'object_key')
    start_date_str = config.get('main', 'start_date')
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')

    end_date = end_date = datetime(2025, 6, 30)
    delta = timedelta(weeks=1)

    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
    )

    csv_content = []
    csv_content.insert(0, ['Date', 'Song', 'Artist', 'Rank', 'Last Week', 'Peak Position', 'Weeks in Charts', 'Image URL'])

    with requests.Session() as session:
        for date_str in generate_dates(start_date, end_date, delta):
            csv_content.extend(process_date(session, date_str, bucket_name, url, object_key))

    csv_content_str = '\n'.join(','.join(map(str, row)) for row in csv_content)
    upload_to_minio(csv_content_str, bucket_name, object_key, s3_client)

if __name__ == '__main__':
    main()