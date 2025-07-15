#script to generate youtube api data and upload to s3
import json
import datetime
from googleapiclient.discovery import build
import boto3

#insert api key
API_KEY = ''
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

S3_BUCKET = 'raw-youtube-data-9'           # replace with your bucket name
S3_RAW_PREFIX = 'raw_data/'            # your s3 folder path for raw data

def fetch_youtube_videos(keyword, max_results=50):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

    search_response = youtube.search().list(
        q=keyword,
        part='id,snippet',
        maxResults=max_results,
        order='date',
        type='video',
        publishedAfter=(datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat("T") + "Z"
    ).execute()

    videos = []
    for item in search_response['items']:
        video_id = item['id']['videoId']
        snippet = item['snippet']
        videos.append({
            'video_id': video_id,
            'title': snippet['title'],
            'channel_title': snippet['channelTitle'],
            'publish_time': snippet['publishedAt']
        })
    return videos

def upload_json_lines_to_s3(data, bucket, s3_key):
    s3 = boto3.client('s3')
    # Prepare JSON lines content in memory
    json_lines = '\n'.join(json.dumps(record) for record in data)
    # Upload as bytes
    s3.put_object(Body=json_lines.encode('utf-8'), Bucket=bucket, Key=s3_key)
    print(f"✅ Uploaded raw data to s3://{bucket}/{s3_key}")

if __name__ == "__main__":
    keyword = 'generative AI'
    videos = fetch_youtube_videos(keyword)
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    s3_key = f"{S3_RAW_PREFIX}{today}_youtube_videos.json"
    upload_json_lines_to_s3(videos, S3_BUCKET, s3_key)
    print(f"Uploaded {len(videos)} videos for keyword '{keyword}' to S3")