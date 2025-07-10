import boto3
from kaggle.api.kaggle_api_extended import KaggleApi
from botocore.exceptions import NoCredentialsError
import csv
import os
import io
import asyncio
import aiohttp
from bs4 import BeautifulSoup


bucket_name = 'billboard'
access_key = os.environ.get('MINIO_ACCESS_KEY', '')
secret_key = os.environ.get('MINIO_SECRET_KEY', '')
kaggle_folder = "/home/ubuntu/kaggle/billboard"

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)

def append_to_whole_file(collection_key, collection_week_key):
    try:
        global_csv = asyncio.run(download_from_minio(collection_key))
        current_date_csv = asyncio.run(download_from_minio(collection_week_key))

        global_csv.extend(current_date_csv[1:])
        updated_csv_content_str = '\n'.join(','.join(map(str, row)) for row in global_csv)

        s3_client.put_object(Body=updated_csv_content_str, Bucket=bucket_name, Key=collection_key)
        print(f"Content appended to {bucket_name}/{collection_key}")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"Failed to append content to {bucket_name}/{collection_key}: {str(e)}")


async def download_from_minio(object_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        reader = csv.reader(io.StringIO(content))
        existing_content = list(reader)
        return existing_content
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"Failed to download content from {bucket_name}/{object_key}: {str(e)}")
        return []

async def upload_to_minio(csv_content, object_key):
    try:
        csv_content_str = '\n'.join(','.join(map(str, row)) for row in csv_content)
        s3_client.put_object(Body=csv_content_str, Bucket=bucket_name, Key=object_key)
        print(f"Content uploaded to {bucket_name}/{object_key}")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"Failed to upload content to {bucket_name}/{object_key}: {str(e)}")

def download_file_localy(s3, bucket_path):
    try:
        file_name = os.path.basename(bucket_path)
        local_path = os.path.join(kaggle_folder, file_name)

        s3.download_file(bucket_name, bucket_path, local_path)
        print(f"File '{bucket_path}' downloaded successfully to '{local_path}'.")
    except Exception as e:
        print(f"Error downloading file '{bucket_path}': {e}")

async def process_date(date_str,charts='hot-100'):
    url = f'https://www.billboard.com/charts/{charts}/{date_str}'

    async with aiohttp.ClientSession() as session:
        html_content = await fetch(session, url)

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

        year, month, day = map(int, date_str.split('-'))
        collection_week_key = f"{charts}/{year:04d}/{month:02d}/{day:02d}/result.csv"
        await upload_to_minio(csv_content, collection_week_key)

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()