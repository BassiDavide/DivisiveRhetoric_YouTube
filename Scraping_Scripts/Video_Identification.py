import csv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import isodate

# Initialize the YouTube API client
youtube = build('youtube', 'v3', developerKey='ADD_YOUTUBE_API_KEY')


def youtube_search(queries, min_views=1, max_results=50, published_after="2013-01-01T00:00:00Z",
                   published_before="2024-05-01T00:00:00Z", sort_by='viewCount'):
    videos = []
    total_videos_processed = 0

    for query in queries:
        query_videos = []  # Track videos for this specific query
        next_page_token = None

        while len(query_videos) < max_results:  # Changed to use max_results directly for each query
            try:
                search_response = youtube.search().list(
                    q=query,
                    part='id,snippet',
                    maxResults=50,
                    type='video',
                    order=sort_by,
                    relevanceLanguage='en',
                    regionCode='US',
                    publishedAfter=published_after,
                    publishedBefore=published_before,
                    pageToken=next_page_token
                ).execute()

                video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]

                if video_ids:
                    try:
                        video_response = youtube.videos().list(
                            part='snippet,statistics,contentDetails',
                            id=','.join(video_ids)
                        ).execute()

                        for video in video_response.get('items', []):
                            try:
                                category_id = video.get('snippet', {}).get('categoryId', '')

                                if category_id == '25':
                                    duration = video.get('contentDetails', {}).get('duration', 'PT0M0S')
                                    view_count = int(video.get('statistics', {}).get('viewCount', 0))
                                    comment_count = int(video.get('statistics', {}).get('commentCount', 0))

                                    video_info = {
                                        'title': video.get('snippet', {}).get('title', 'No Title'),
                                        'url': f"https://www.youtube.com/watch?v={video.get('id', '')}",
                                        'views': view_count,
                                        'comments': comment_count,
                                        'duration': duration,
                                        'channel': video.get('snippet', {}).get('channelTitle', 'No Channel'),
                                        'category_id': category_id,
                                        'query': query
                                    }
                                    query_videos.append(video_info)

                            except Exception as e:
                                print(f"Error processing video: {e}")
                                print(f"Video data: {video}")
                                continue

                        total_videos_processed += len(video_response.get('items', []))

                    except Exception as e:
                        print(f"Error getting video details: {e}")
                        continue

                next_page_token = search_response.get('nextPageToken')
                if not next_page_token:
                    break

            except HttpError as e:
                error = e.resp.status
                if error == 403:
                    print("Quota exceeded. Waiting for quota reset...")
                    time.sleep(3600)
                else:
                    print(f"An HTTP error {error} occurred:\n{e}")
                    break

            if len(query_videos) >= max_results:
                break

        videos.extend(query_videos)
        print(f"Collected {len(query_videos)} videos for query: {query}")

    # Sort all videos by view count before returning
    videos.sort(key=lambda x: x['views'], reverse=True)
    return videos, total_videos_processed


def filter_videos(videos, min_views=1, min_comments=1000, max_duration='PT20M'):
    filtered_videos = []
    max_duration_seconds = isodate.parse_duration(max_duration).total_seconds()
    for video in videos:
        duration = isodate.parse_duration(video['duration']).total_seconds()
        view_count = video['views']
        comment_count = video['comments']

        # Debugging prints
        print(f"Checking video: {video['title']}")
        print(f"Duration (seconds): {duration}, Views: {view_count}, Comments: {comment_count}")

        if view_count >= min_views and comment_count >= min_comments and duration <= max_duration_seconds:
            filtered_videos.append(video)
        else:
            print(f"Excluded video: {video['title']}")

    # Sort filtered videos by view count
    filtered_videos.sort(key=lambda x: x['views'], reverse=True)
    return filtered_videos


def remove_duplicates(videos):
    seen_urls = set()
    unique_videos = []
    for video in videos:
        if video['url'] not in seen_urls:
            unique_videos.append(video)
            seen_urls.add(video['url'])

    # Sort unique videos by view count
    unique_videos.sort(key=lambda x: x['views'], reverse=True)
    return unique_videos


def save_to_csv(videos, filename):
    # Get all possible keys from all dictionaries
    keys = set()
    for video in videos:
        keys.update(video.keys())

    # Convert to sorted list for consistent column order
    keys = sorted(list(keys))

    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(videos)


# Immigration
queries = [
    'immigration',
    'migration crisis',
    'asylum refugees seeker',
    'border control',
    'migrant welcoming',
    'solidarity migrants',
]

# Climate
#queries = [
#    'climate change'
#    'global warming'
#    'climate activism'
#    'climate policies'
#    'climate change hoax'
#    'eco anxiety'
#]

# Retrieve all videos (50 per query)
all_videos, total_videos_processed = youtube_search(queries, max_results=50)

# Save unfiltered videos to a CSV file
#save_to_csv(all_videos, "ADD_PATH_FOR_CSV_FILE")

# Remove duplicates from the list of all videos
unique_videos = remove_duplicates(all_videos)

# Apply filtering criteria
filtered_videos = filter_videos(unique_videos, min_views=1, min_comments=500, max_duration='PT15M')

# Save filtered videos to a CSV file
save_to_csv(filtered_videos, "ADD_PATH_FOR_CSV_FILE")

print(f"Total videos processed: {total_videos_processed}")
print(f"Total unique videos: {len(unique_videos)}")
print(f"Total filtered videos: {len(filtered_videos)}")

for video in filtered_videos:
    print(
        f"{video['title']} - {video['url']} (Views: {video['views']}, Comments: {video['comments']}, Duration: {video['duration']}")
