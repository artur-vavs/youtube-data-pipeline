# %%
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()

API_KEY = os.getenv("API_KEY")

CHANNELS = ["@ingresso-com","@CNNbrasil"]

youtube = build('youtube', 'v3', developerKey=API_KEY)
# %%
def get_chanel_data(channel):

    

    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        forHandle=channel
    )

    response = request.execute()

    return response

def generate_channel_data(json):

    data_items = json['items'][0]
    data_snippet = data_items['snippet']
    data_statistics = data_items['statistics']
    
    id = data_items['id']
    title = data_snippet['title']
    publishedAt = data_snippet['publishedAt']
    country = data_snippet['country']
    viewCount = data_statistics['viewCount']
    subscriberCount = data_statistics['subscriberCount']
    videoCount = data_statistics['videoCount']
    id_playlist = data_items["contentDetails"]["relatedPlaylists"]["uploads"]

    dict = {
            'id': id,
            'title': title,
            'publishedAt': publishedAt,
            'country': country,
            'viewCount': viewCount,
            'subscriberCount':subscriberCount,
            'videoCount' : videoCount,
            'id_playlist': id_playlist
        }
    
    return dict

def generate_dataframe(dict_list: list) -> pd.DataFrame:
    df = pd.DataFrame(dict_list)
    return df

dict_list = []
for channel in CHANNELS:
    data_channel = get_chanel_data(channel)
    dict_data = generate_channel_data(data_channel)
    dict_list.append(
    dict_data
    )

df = generate_dataframe(dict_list)

df.head()

# %%
def get_playlist_videos(playlist_id):

    request = youtube.playlistItems().list(
        part='snippet,contentDetails',
        playlistId=playlist_id,
        maxResults=50,
    )

    response = request.execute()

    return response

def generate_videos_data(json):
    video_items = json['items'][0]

# %%
json_ingresso = get_chanel_data("@ingresso-com")
data_ingresso = generate_channel_data(json_ingresso)
playlist_id = data_ingresso['id_playlist']
# %%
playlist_data = get_playlist_videos(playlist_id)
print(playlist_data)
# %%
for i in playlist_data['items']:
    print(f"publishedAt: {i['snippet']['publishedAt']} | channelId: {i['snippet']['channelId']}")
    #print(i['snippet']['channelId'])
    print(i['snippet']['title'])
    print(i['snippet']['playlistId'])
    print(i['contentDetails']['videoId'])
    print(i['contentDetails']['videoPublishedAt'])

#print(items_playlist)