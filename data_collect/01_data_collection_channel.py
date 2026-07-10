# %%
import os
from collections.abc import Iterable, Iterator
from typing import Any
import fastparquet
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLES = ["@tseries"]

CHANNEL_PARTS = "snippet,contentDetails,statistics"
PLAYLIST_PARTS = "snippet,contentDetails"
VIDEO_PARTS = "snippet,contentDetails,statistics"
MAX_PLAYLIST_PAGES = 5


def get_youtube_client(api_key: str | None):
    if not api_key:
        raise ValueError("API_KEY nao encontrada no arquivo .env.")

    return build("youtube", "v3", developerKey=api_key)


def chunked(values: Iterable[str], batch_size: int) -> Iterator[list[str]]:
    batch: list[str] = []

    for value in values:
        batch.append(value)
        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def request_channel_by_handle(youtube, channel_handle: str) -> dict[str, Any]:
    try:
        response = youtube.channels().list(
            part=CHANNEL_PARTS,
            forHandle=channel_handle,
        ).execute()
    except HttpError as exc:
        raise RuntimeError(
            f"Erro ao consultar o canal {channel_handle}: {exc}"
        ) from exc

    items = response.get("items", [])
    if not items:
        raise ValueError(f"Nenhum canal encontrado para o handle {channel_handle}.")

    return items[0]


def transform_channel_item(
    channel_item: dict[str, Any], channel_handle: str
) -> dict[str, Any]:
    snippet = channel_item.get("snippet", {})
    statistics = channel_item.get("statistics", {})
    content_details = channel_item.get("contentDetails", {})
    related_playlists = content_details.get("relatedPlaylists", {})

    return {
        "channel_handle": channel_handle,
        "channel_id": channel_item.get("id"),
        "channel_title": snippet.get("title"),
        "channel_published_at": snippet.get("publishedAt"),
        "channel_country": snippet.get("country"),
        "view_count": statistics.get("viewCount"),
        "subscriber_count": statistics.get("subscriberCount"),
        "video_count": statistics.get("videoCount"),
        "uploads_playlist_id": related_playlists.get("uploads"),
    }


def list_playlist_items(youtube, playlist_id: str, max_pages: int | None = None) -> list[dict[str, Any]]:
    playlist_items: list[dict[str, Any]] = []
    next_page_token: str | None = None
    current_page = 0
    while True:
        try:
            response = youtube.playlistItems().list(
                part=PLAYLIST_PARTS,
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            ).execute()
        except HttpError as exc:
            raise RuntimeError(
                f"Erro ao consultar a playlist {playlist_id}: {exc}"
            ) from exc

        playlist_items.extend(response.get("items", []))

        current_page +=1 

        if max_pages is not None and current_page >= max_pages:
            break

        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    return playlist_items


def transform_playlist_item(
    playlist_item: dict[str, Any],
    channel_record: dict[str, Any],
) -> dict[str, Any]:
    snippet = playlist_item.get("snippet", {})
    content_details = playlist_item.get("contentDetails", {})

    return {
        "source_channel_handle": channel_record["channel_handle"],
        "source_channel_id": channel_record["channel_id"],
        "source_channel_title": channel_record["channel_title"],
        "uploads_playlist_id": channel_record["uploads_playlist_id"],
        "playlist_item_id": playlist_item.get("id"),
        "playlist_item_position": snippet.get("position"),
        "playlist_item_published_at": snippet.get("publishedAt"),
        "video_id": content_details.get("videoId"),
        "video_published_at": content_details.get("videoPublishedAt"),
    }


def list_video_details(youtube, video_ids: Iterable[str]) -> list[dict[str, Any]]:
    video_items: list[dict[str, Any]] = []
    valid_video_ids = [video_id for video_id in video_ids if video_id]

    for batch in chunked(valid_video_ids, batch_size=50):
        try:
            response = youtube.videos().list(
                part=VIDEO_PARTS,
                id=",".join(batch),
                maxResults=50,
            ).execute()
        except HttpError as exc:
            raise RuntimeError(
                f"Erro ao consultar detalhes dos videos: {exc}"
            ) from exc

        video_items.extend(response.get("items", []))

    return video_items


def transform_video_item(
    video_item: dict[str, Any],
    playlist_record: dict[str, Any],
) -> dict[str, Any]:
    snippet = video_item.get("snippet", {})
    statistics = video_item.get("statistics", {})
    content_details = video_item.get("contentDetails", {})

    return {
        **playlist_record,
        "video_title": snippet.get("title"),
        "video_description": snippet.get("description"),
        "video_channel_id": snippet.get("channelId"),
        "video_channel_title": snippet.get("channelTitle"),
        "video_duration": content_details.get("duration"),
        "video_definition": content_details.get("definition"),
        "video_caption": content_details.get("caption"),
        "view_count": statistics.get("viewCount"),
        "like_count": statistics.get("likeCount"),
        "favorite_count": statistics.get("favoriteCount"),
        "comment_count": statistics.get("commentCount"),
    }


def build_dataframe(
    records: list[dict[str, Any]],
    datetime_columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
) -> pd.DataFrame:
    dataframe = pd.DataFrame(records)

    if dataframe.empty:
        return dataframe

    for column in datetime_columns or []:
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(
                dataframe[column], errors="coerce", utc=True
            )

    for column in numeric_columns or []:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(
                dataframe[column], errors="coerce"
            ).astype("Int64")

    return dataframe


def extract_channel_records(
    youtube,
    channel_handles: list[str],
) -> list[dict[str, Any]]:
    channel_records: list[dict[str, Any]] = []

    for channel_handle in channel_handles:
        channel_item = request_channel_by_handle(youtube, channel_handle)
        channel_records.append(transform_channel_item(channel_item, channel_handle))

    return channel_records


def extract_video_records(
    youtube,
    channel_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    playlist_records: list[dict[str, Any]] = []

    for channel_record in channel_records:
        playlist_id = channel_record.get("uploads_playlist_id")
        if not playlist_id:
            continue

        playlist_items = list_playlist_items(youtube, playlist_id, MAX_PLAYLIST_PAGES)
        for playlist_item in playlist_items:
            playlist_records.append(
                transform_playlist_item(playlist_item, channel_record)
            )

    playlist_by_video_id = {
        record["video_id"]: record for record in playlist_records if record["video_id"]
    }

    video_items = list_video_details(youtube, playlist_by_video_id.keys())

    return [
        transform_video_item(video_item, playlist_by_video_id[video_item["id"]])
        for video_item in video_items
        if video_item.get("id") in playlist_by_video_id
    ]


def run_channel_video_etl(
    channel_handles: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    youtube = get_youtube_client(API_KEY)

    channel_records = extract_channel_records(youtube, channel_handles)
    video_records = extract_video_records(youtube, channel_records)

    channels_df = build_dataframe(
        records=channel_records,
        datetime_columns=["channel_published_at"],
        numeric_columns=["view_count", "subscriber_count", "video_count"],
    )

    videos_df = build_dataframe(
        records=video_records,
        datetime_columns=["playlist_item_published_at", "video_published_at"],
        numeric_columns=[
            "playlist_item_position",
            "view_count",
            "like_count",
            "favorite_count",
            "comment_count",
        ],
    )

    return channels_df, videos_df


# %%
if __name__ == "__main__":
    channels_df, videos_df = run_channel_video_etl(CHANNEL_HANDLES)
    print(channels_df.head())
    #channels_df.to_parquet("datalake/bronze/channels_tseries.parquet", index=False)
    #print(videos_df.head(50))
