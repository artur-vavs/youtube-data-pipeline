# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "fastparquet>=2026.5.0",
#     "google-api-python-client>=2.198.0",
#     "python-dotenv>=1.2.2",
#     "pandas>=3.0.0",
# ]
# ///

# %%
"""Carga inicial (camada bronze) de canais pre-definidos por categoria.

Cada canal e coletado atraves do seu handle e gravado em um arquivo parquet
proprio dentro de ``datalake/bronze``. A coleta usa um ThreadPoolExecutor para
paralelizar as chamadas a API do YouTube.

Nota sobre concorrencia:
    - Cada thread recebe exatamente um par (categoria, handle), portanto duas
      threads nunca coletam o mesmo canal (nao ha disputa pelo mesmo dado).
    - O cliente do googleapiclient usa httplib2, que NAO e thread-safe. Por
      isso cada thread constroi o seu proprio cliente em ``collect_channel``,
      evitando conflito de estado compartilhado entre threads.
    - Cada canal grava em um arquivo parquet distinto, entao nao ha disputa de
      escrita no mesmo arquivo.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

API_KEY = os.getenv("API_KEY")

CHANNEL_PARTS = "snippet,contentDetails,statistics"
BRONZE_DIR = Path("datalake/bronze")
MAX_WORKERS = 4
TOP_N = 5

# Canais pre-definidos por categoria, referenciados pelo handle do canal.
CHANNELS_BY_CATEGORY: dict[str, list[str]] = {
    "esporte": ["@ESPN", "@CazeTV", "@Flamengo"],
    "entretenimento": ["@tseries", "@MrBeast", "@FelipeNeto"],
    "games": ["@PewDiePie", "@Cellbit", "@RezendeEvil"],
}


def get_youtube_client(api_key: str | None):
    """Constroi um cliente da YouTube Data API v3."""
    if not api_key:
        raise ValueError("API_KEY nao encontrada no arquivo .env.")

    return build("youtube", "v3", developerKey=api_key)


def request_channel_by_handle(youtube, channel_handle: str) -> dict[str, Any]:
    """Consulta os dados de um canal a partir do seu handle."""
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
    channel_item: dict[str, Any],
    channel_handle: str,
    category: str,
) -> dict[str, Any]:
    """Achata o item de canal da API em um registro tabular."""
    snippet = channel_item.get("snippet", {})
    statistics = channel_item.get("statistics", {})
    content_details = channel_item.get("contentDetails", {})
    related_playlists = content_details.get("relatedPlaylists", {})

    return {
        "category": category,
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


def build_dataframe(
    records: list[dict[str, Any]],
    datetime_columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Cria um DataFrame com tipagem de datas e colunas numericas."""
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


def bronze_path(channel_handle: str) -> Path:
    """Retorna o caminho parquet da camada bronze para um canal."""
    slug = channel_handle.lstrip("@").lower()
    return BRONZE_DIR / f"channels_{slug}.parquet"


def save_channel_bronze(record: dict[str, Any]) -> Path:
    """Grava a carga inicial de um unico canal na camada bronze."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    dataframe = build_dataframe(
        records=[record],
        datetime_columns=["channel_published_at"],
        numeric_columns=["view_count", "subscriber_count", "video_count"],
    )

    destination = bronze_path(record["channel_handle"])
    dataframe.to_parquet(destination, engine="fastparquet", index=False)

    return destination


def collect_channel(category: str, channel_handle: str) -> dict[str, Any]:
    """Coleta e persiste um unico canal (executado por uma thread).

    Constroi o proprio cliente para garantir isolamento entre as threads.
    """
    youtube = get_youtube_client(API_KEY)
    channel_item = request_channel_by_handle(youtube, channel_handle)
    record = transform_channel_item(channel_item, channel_handle, category)
    record["bronze_path"] = str(save_channel_bronze(record))

    return record


def collect_all_channels(
    channels_by_category: dict[str, list[str]],
    max_workers: int = MAX_WORKERS,
) -> list[dict[str, Any]]:
    """Coleta todos os canais em paralelo, um par (categoria, handle) por thread."""
    tasks = [
        (category, handle)
        for category, handles in channels_by_category.items()
        for handle in handles
    ]

    records: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(collect_channel, category, handle): (category, handle)
            for category, handle in tasks
        }

        for future in as_completed(futures):
            category, handle = futures[future]
            try:
                records.append(future.result())
            except (RuntimeError, ValueError) as exc:
                print(f"[falha] {category}/{handle}: {exc}")

    return records


def build_top_channels(
    records: list[dict[str, Any]],
    top_n: int = TOP_N,
) -> pd.DataFrame:
    """Monta o ranking dos canais com mais inscritos."""
    dataframe = build_dataframe(
        records=records,
        datetime_columns=["channel_published_at"],
        numeric_columns=["view_count", "subscriber_count", "video_count"],
    )

    if dataframe.empty:
        return dataframe

    ranking = dataframe.sort_values(
        "subscriber_count", ascending=False, na_position="last"
    ).head(top_n)
    ranking = ranking.reset_index(drop=True)
    ranking.insert(0, "rank", ranking.index + 1)

    return ranking


# %%
if __name__ == "__main__":
    channel_records = collect_all_channels(CHANNELS_BY_CATEGORY)
    top_channels = build_top_channels(channel_records, TOP_N)

    columns = ["rank", "category", "channel_title", "channel_handle", "subscriber_count"]
    print(f"\nTop {TOP_N} canais por numero de inscritos:\n")
    print(top_channels[columns].to_string(index=False))
