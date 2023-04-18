import os
import json
import tempfile
import requests
from bs4 import BeautifulSoup
import boto3
from datetime import datetime
import pytz

def save_to_temp_file(metadata_list):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(json.dumps(metadata_list, ensure_ascii=False).encode('utf-8'))
        return temp_file.name

def scrape_metadata(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    entrylist_contents_main_list = soup.find_all(class_='entrylist-contents-main')

    metadata_list = []

    for entry in entrylist_contents_main_list:
        entrylist_contents_title = entry.find(class_='entrylist-contents-title')
        title_anchor = entrylist_contents_title.find('a')
        
        entrylist_contents_domain = entry.find(class_='entrylist-contents-domain')
        domain_anchor = entrylist_contents_domain.find('a')
        
        entrylist_contents_meta = entry.find(class_='entrylist-contents-meta')
        category_anchor = entrylist_contents_meta.find(class_='entrylist-contents-category').find('a')
        contents_date = entrylist_contents_meta.find(class_='entrylist-contents-date').get_text(strip=True)
        
        entrylist_contents_users = entry.find(class_='entrylist-contents-users')
        bookmark_users = entrylist_contents_users.find('a').find('span').get_text(strip=True)
        
        metadata = {
            'title': title_anchor.get_text(strip=True),
            'url': title_anchor['href'],
            'domain': domain_anchor.get_text(strip=True),
            'category': category_anchor.get_text(strip=True),
            'contents_published_date': contents_date,
            'bookmark_users': bookmark_users
        }
        metadata_list.append(metadata)

    return metadata_list

def upload_to_s3(bucket, file_path, key):
    profile = os.environ.get('AWS_PROFILE')

    if profile:
        session = boto3.Session(profile_name=profile)
        s3 = session.client('s3')
    else:
        s3 = boto3.client('s3')

    s3.upload_file(file_path, bucket, key)

def lambda_handler(event, context):
    url = "https://b.hatena.ne.jp/hotentry/it"
    metadata_list = scrape_metadata(url)
    temp_file_path = save_to_temp_file(metadata_list)
    
    s3 = boto3.client('s3')
    bucket = os.environ['S3_BUCKET_NAME']
    
    tokyo_tz = pytz.timezone('Asia/Tokyo')
    now = datetime.now(tokyo_tz)
    date_str = now.strftime('%Y%m%d')
    
    key = f'hatenablog/hatenablog_hotentry_{date_str}.json'
    
    upload_to_s3(bucket, temp_file_path, key)
        
    os.remove(temp_file_path)
    return {'statusCode': 200, 'body': json.dumps('Scraping and upload to S3 completed!')}