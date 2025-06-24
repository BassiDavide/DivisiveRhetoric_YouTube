import pandas as pd
import os
import json
from googleapiclient.discovery import build
import time

file_path = 'ADD_CSV_FILE_PATH_VIDEO_LIST'
videos_df = pd.read_csv(file_path)

api_key = 'ADD_YOUTUBE_API_KEY'
youtube = build('youtube', 'v3', developerKey=api_key)

output_path = 'ADD_FOLDER_PATH_FOR_PROCESSED_FILES'
os.makedirs(output_path, exist_ok=True)


def fetch_replies(youtube, json_file, parent_id, thread_id, video_id, video_metadata):
    page_token = None
    while True:
        try:
            replies_response = youtube.comments().list(
                part='snippet',
                parentId=parent_id,
                maxResults=100,
                pageToken=page_token,
                textFormat='plainText'
            ).execute()

            for reply in replies_response.get('items', []):
                json_file.write(json.dumps({
                    'CommentID': reply['id'],
                    'ThreadID': thread_id,
                    'VideoID': video_id,
                    'ParentCommentID': parent_id,
                    'CommentText': reply['snippet']['textDisplay'],
                    'AuthorName': reply['snippet']['authorDisplayName'],
                    'NumberOfLikes': reply['snippet']['likeCount'],
                    'IsReply': 'True',
                    'Timestamp': reply['snippet']['publishedAt'],
                    'Period': video_metadata['period'],
                    'ChannelLeaning': video_metadata['channel_leaning'],
                    'Source': video_metadata['Source']
                }) + '\n')

            page_token = replies_response.get('nextPageToken')
            if not page_token:
                break

        except Exception as e:
            print(f"Error fetching replies for parent comment {parent_id}: {e}")
            time.sleep(10)  # Wait if we hit an error (like quota limit)
            break


# Track quota usage
quota_used = 0
quota_limit = 10000
quota_file = os.path.join(output_path, 'quota_usage.txt')

# Load previous quota if file exists
if os.path.exists(quota_file):
    with open(quota_file, 'r') as f:
        quota_used = int(f.read().strip())
    print(f"Resuming with quota usage: {quota_used}/{quota_limit}")

for index, row in videos_df.iterrows():
    video_id = row['video_id']

    # Skip if we're close to quota limit
    if quota_used >= quota_limit - 10:
        print(f"Daily quota limit approaching: {quota_used}/{quota_limit}. Stopping.")
        break

    video_metadata = {
        'period': row['period'],
        'channel_leaning': row['channel_leaning'],
        'Source': row['Source']
    }

    comments_file_path = os.path.join(output_path, f'comments_{video_id}.jsonl')


    if os.path.exists(comments_file_path) and os.path.getsize(comments_file_path) > 0:
        print(f"Skipping already processed video: {video_id}")
        continue

    print(f"Processing video {video_id} ({index + 1}/{len(videos_df)})")

    with open(comments_file_path, 'w', encoding='utf-8') as comments_file:
        page_token = None
        while True:
            try:
                # Update quota tracking (commentThreads.list = 1 unit)
                quota_used += 1
                with open(quota_file, 'w') as f:
                    f.write(str(quota_used))

                top_level_comments_response = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=100,
                    pageToken=page_token,
                    order='relevance',
                    textFormat='plainText'
                ).execute()

                for item in top_level_comments_response.get('items', []):
                    thread_id = item['id']
                    top_level_comment = item['snippet']['topLevelComment']
                    comment_id = top_level_comment['id']
                    comments_file.write(json.dumps({
                        'CommentID': comment_id,
                        'ThreadID': thread_id,
                        'VideoID': video_id,
                        'ParentCommentID': '',
                        'CommentText': top_level_comment['snippet']['textDisplay'],
                        'AuthorName': top_level_comment['snippet']['authorDisplayName'],
                        'NumberOfLikes': top_level_comment['snippet']['likeCount'],
                        'IsReply': 'False',
                        'Timestamp': top_level_comment['snippet']['publishedAt'],
                        'Period': video_metadata['period'],
                        'ChannelLeaning': video_metadata['channel_leaning'],
                        'Source': video_metadata['Source']
                    }) + '\n')

                    # Check if there are replies
                    if item['snippet']['totalReplyCount'] > 0:
                        # Update quota tracking (comments.list = 1 unit)
                        quota_used += 1
                        with open(quota_file, 'w') as f:
                            f.write(str(quota_used))

                        fetch_replies(youtube, comments_file, comment_id, thread_id, video_id, video_metadata)

                # Check if we need to stop due to quota
                if quota_used >= quota_limit - 10:
                    print(f"Daily quota limit approaching: {quota_used}/{quota_limit}. Stopping.")
                    break

                page_token = top_level_comments_response.get('nextPageToken')
                if not page_token:
                    break

            except Exception as e:
                print(f"Error fetching comments for video {video_id}: {e}")
                time.sleep(10)  # Wait if we hit an error (like quota limit)
                break

    print(f"Comments collected for video {video_id} (Quota used: {quota_used}/{quota_limit})")

print("Data collection complete.")
