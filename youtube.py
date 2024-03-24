from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from IPython.display import JSON
from dotenv import dotenv_values
import psycopg2
from sqlalchemy import create_engine
import isodate

secrets = dotenv_values("userdata.env")
api_key = secrets["api_key"]

channel_ids = ['UCXuqSBlHAE6Xw-yeJA0Tunw', # linus tech tips
               'UCMiJRAwDNSNzuYeN2uWa0pA', # mrwhostheboss
               'UCBJycsmduvYEL83R_U4JriQ', # marques brownlee
               'UCey_c7U86mJGz1VJWH5CYPA', # ijustine
               'UCXGgrKt94gR6lmN4aN3mYTg' # austin evans
                ]
# Insert YouTube api version and api_key, code from YouTube documentation
youtube = build(
    'youtube', 'v3', developerKey = api_key)

request = youtube.channels().list(
    part = "snippet,contentDetails,statistics",
    id = ','.join(channel_ids)
)
response = request.execute()

# Loop through items to get snippet, statistic and contentdetail lists
def get_channel_stats(youtube, channel_ids):
    all_data = []
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = ','.join(channel_ids))
    response = request.execute()
    for i in range(len(response['items'])):
        data = {'channel_name': response['items'][i]['snippet']['title'],
                'subscribers' : response['items'][i]['statistics']['subscriberCount'],
                'views' : response['items'][i]['statistics']['viewCount'],
                'total_videos' : response['items'][i]['statistics']['videoCount'],
                'playlist_id' : response['items'][i]['contentDetails']['relatedPlaylists']['uploads']}

        all_data.append(data)
    return all_data

channel_stats = get_channel_stats(youtube, channel_ids)
channel_data = pd.DataFrame(channel_stats)

channel_data['subscribers'] = pd.to_numeric(channel_data['subscribers'])
channel_data['views'] = pd.to_numeric(channel_data['views'])
channel_data['total_videos'] = pd.to_numeric(channel_data['total_videos'])

playlist_id = str(channel_data.loc[channel_data['channel_name'] == 'Linus Tech Tips', 'playlist_id'])
playlist_id = playlist_id[5:29] # Strip linus tech tip's playlist id

def get_video_ids(youtube, playlist_id):
    video_ids = []

    next_page_token = None
    while True:
        request = youtube.playlistItems().list(
            part = "snippet, contentDetails",
            playlistId = playlist_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])

        # Check if there are more pages to fetch
        next_page_token = response.get('nextPageToken')
        if next_page_token == None:
            break  

    return video_ids

get_video_ids(youtube, playlist_id)

video_ids = get_video_ids(youtube, playlist_id)

def get_video_details(youtube, video_ids):
    all_video_info = []
    # from 0-50, for 'i' we extract 50 then move on to the next page and so on
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics",
            id = ','.join(video_ids[i:i+50])
        )
        response = request.execute()
        # get None if there are no values that return
        for video in response['items']:
            video_info = {
                'video_id': video['id'],
                'channel_title': video['snippet']['channelTitle'],
                'title': video['snippet']['title'],
                'description': video['snippet'].get('description', None),
                'published_at': video['snippet']['publishedAt'],
                'view_count': video['statistics'].get('viewCount', None),
                'like_count': video['statistics'].get('likeCount', None),
                'comment_count': video['statistics'].get('commentCount', None),
                'duration': video['contentDetails'].get('duration', None),
            }
            all_video_info.append(video_info)

    return pd.DataFrame(all_video_info)

video_data = get_video_details(youtube, video_ids)

video_data['published_at']=pd.to_datetime(video_data['published_at']).dt.date
video_data['view_count'] = pd.to_numeric(video_data['view_count'])
video_data['like_count'] = pd.to_numeric(video_data['like_count'])
video_data['comment_count'] = pd.to_numeric(video_data['comment_count'])

# https://stackoverflow.com/questions/16742381/how-to-convert-youtube-api-duration-to-seconds
# https://www.youtube.com/watch?v=D56_Cx36oGY
# Convert YouTube duration string to seconds
video_data['duration_secs'] = video_data['duration'].apply(lambda x: isodate.parse_duration(x).total_seconds())


conn = psycopg2.connect(
    dbname = secrets["dbname"],
    user = secrets["user"],
    password = secrets["password"],
    host = secrets["host"],
    port = secrets["port"]
)

print("Connection to the database successful!")

engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + dbname)

channel_data.to_sql('ay_channel_data', engine, schema='student', if_exists='replace', index=False)
print("Channel_data DataFrame successfully updated!")

video_df.to_sql('ay_video_df', engine, schema='student', if_exists='replace', index=False)
print("Video_data DataFrame successfully updated!")


conn.close()
