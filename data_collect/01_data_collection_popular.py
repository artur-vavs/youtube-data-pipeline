# %%
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")


params = {
    "part": "snippet,statistics",
    "chart": "mostPopular",
    "regionCode": "BR",   # BR, US, JP, etc.
    "videoCategoryId": 20,
    "maxResults": 15,
    "key": API_KEY,
}


response = requests.get(
    "https://www.googleapis.com/youtube/v3/videos",
    params=params
)

data = response.json()



youtube_categories = {
    1: "Film & Animation",
    2: "Autos & Vehicles",
    10: "Music",
    15: "Pets & Animals",
    17: "Sports",
    18: "Short Movies",
    19: "Travel & Events",
    20: "Gaming",
    21: "Videoblogging",
    22: "People & Blogs",
    23: "Comedy",
    24: "Entertainment",
    25: "News & Politics",
    26: "Howto & Style",
    27: "Education",
    28: "Science & Technology",
    29: "Nonprofits & Activism",
    30: "Movies",
    31: "Anime/Animation",
    32: "Action/Adventure",
    33: "Classics",
    34: "Comedy",
    35: "Documentary",
    36: "Drama",
    37: "Family",
    38: "Foreign",
    39: "Horror",
    40: "Sci-Fi/Fantasy",
    41: "Thriller",
    42: "Shorts",
    43: "Shows",
    44: "Trailers",
}

def obter_id(dados_json):
    id = dados_json['id']
    return id

def obter_dados_snippet(dados_json):
    snippet = dados_json['snippet']
    publishedAt = snippet['publishedAt']
    channelId = snippet['channelId']
    categoryId = snippet['categoryId']
    return publishedAt, channelId, categoryId
    
def obter_dados_statistics(dados_json):
    statistics = dados_json['statistics']
    viewCount = statistics['viewCount']
    likeCount = statistics['likeCount']
    favoriteCount = statistics['favoriteCount']
    commentCount = statistics['commentCount']
    return viewCount, likeCount, favoriteCount, commentCount

# %% 
dados_video = []
for video in (data['items']):
    id = obter_id(video)
    publishedAt, channelId, categoryId = obter_dados_snippet(video)
    viewCount, likeCount, favoriteCount, commentCount = obter_dados_statistics(video)
    dict_data = {
    'id': id,
    'channelId': channelId,
    'publishedAt': publishedAt,
    'categoryId': categoryId,
    'viewCount': viewCount,
    'likeCount': likeCount,
    'favoriteCount': favoriteCount,
    'commentCount': commentCount,
    }
    dados_video.append(dict_data)
df = pd.DataFrame(dados_video)

# %%
df.head(20)