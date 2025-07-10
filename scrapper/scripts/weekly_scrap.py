import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import boto3
from botocore.exceptions import NoCredentialsError
import asyncio
import argparse
import configparser
from utils import generate_dates

access_key = os.environ.get('MINIO_ACCESS_KEY', '')
secret_key = os.environ.get('MINIO_SECRET_KEY', '')

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)
bucket_name = 'billboard'

async def fetch(session, url, semaphore):
    async with semaphore:
        async with session.get(url) as response:
            return await response.text()

async def process_date(session, date_str, semaphore, bucket_name, url, object_key_format):
    print(date_str)
    url = f'{url}/{date_str}'
    html_content = await fetch(session, url, semaphore)

    soup = BeautifulSoup(html_content, 'html.parser')
    result = soup.select('.o-chart-results-list-row-container')

    csv_content = [['Date', 'Song', 'Artist', 'Rank', 'Last Week', 'Peak Position', 'Weeks in Charts', 'Image URL']]

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

    object_key = object_key_format.format(year=int(date_str[:4]), month=int(date_str[5:7]), day=int(date_str[8:10]))
    await upload_to_minio(csv_content, bucket_name, date_str, object_key)

async def upload_to_minio(csv_content, bucket_name, date_str, object_key):
    csv_content_str = '\n'.join(','.join(map(str, row)) for row in csv_content)

    try:
        s3_client.put_object(Body=csv_content_str, Bucket=bucket_name, Key=object_key)
        print(f"Content uploaded to {bucket_name}/{object_key}")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"Failed to upload content to {bucket_name}/{object_key}: {str(e)}")


async def main():
    semaphore = asyncio.Semaphore(6)  # Adjust the value based on the desired rate limit

    parser = argparse.ArgumentParser(description='Billboard Scraper')
    parser.add_argument('--config', default='config.ini', help='Path to the configuration file')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    url = config.get('main', 'url')
    object_key_format = config.get('main', 'object_key_format')
    start_date_str = config.get('main', 'start_date')
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')

    end_date = datetime.now()
    delta = timedelta(weeks=1)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for date_str in generate_dates(start_date, end_date, delta):
            tasks.append(process_date(session, date_str, semaphore, bucket_name, url, object_key_format))

        await asyncio.gather(*tasks)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()