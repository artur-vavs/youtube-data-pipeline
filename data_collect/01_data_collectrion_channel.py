# %%
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

CHANNELS = ["@ingresso-com","@CNNbrasil"]

def get_chanel_data(channel):
    params = {
        "part": "snippet,contentDetails,statistics",
        "forHandle": channel,
        "key": API_KEY
    }
    url = "https://www.googleapis.com/youtube/v3/channels"
    
    return requests.get(url, params=params).json()

def generate_dict_data(json):

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

# %%

dict_list = []
for channel in CHANNELS:
    data_channel = get_chanel_data(channel)
    dict_data = generate_dict_data(data_channel)
    dict_list.append(
    dict_data
    )

df = generate_dataframe(dict_list)
# %%
df.head()