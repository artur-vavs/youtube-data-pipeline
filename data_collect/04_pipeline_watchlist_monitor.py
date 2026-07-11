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
"""Pipeline de monitoramento diario de uma watchlist definida pelo cliente.

O cliente informa ate 10 canais que deseja acompanhar (o front-end futuro
gravara essa lista em ``config/watchlists.json``). A cada execucao (6x/dia)
coletamos um snapshot dos canais e de seus videos e cruzamos os dados para
entregar, na camada gold:

    * ranking (top 5 destacado) por numero de inscritos dentro da watchlist;
    * crescimento diario (delta de inscritos/views entre snapshots);
    * taxa de engajamento por canal;
    * cadencia de postagem (videos por semana);
    * movimentacao de rank (subiu / desceu / entrou / saiu do top 5).

Camadas (medallion):
    * bronze : dados brutos do snapshot atual (landing zone; sobrescrita a cada
               run). 1 parquet por canal e 1 por canal-de-videos.
    * silver : dados tratados e ACUMULADOS por ``ingested_at`` (serie temporal,
               idempotente por chave + snapshot).
    * gold   : modelagem dimensional (fatos/dimensoes) + marts analiticos com
               as metricas derivadas.

Concorrencia (tema da branch): cada thread processa um unico canal, constroi o
proprio cliente (httplib2 nao e thread-safe) e grava em arquivo distinto -> sem
disputa pelo mesmo dado nem conflito de escrita.
"""

import json
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

API_KEY = os.getenv("API_KEY")

# --------------------------------------------------------------------------- #
# Configuracao
# --------------------------------------------------------------------------- #
CHANNEL_PARTS = "snippet,contentDetails,statistics"
PLAYLIST_PARTS = "snippet,contentDetails"
VIDEO_PARTS = "snippet,contentDetails,statistics"

MAX_WORKERS = 8
TOP_N = 5
MAX_CHANNELS_PER_CLIENT = 10
MAX_PLAYLIST_PAGES = 2  # ~100 videos mais recentes por canal

CONFIG_PATH = Path("config/watchlists.json")
DATALAKE = Path("datalake")
BRONZE_CHANNELS_DIR = DATALAKE / "bronze" / "channels"
BRONZE_VIDEOS_DIR = DATALAKE / "bronze" / "videos"
SILVER_DIR = DATALAKE / "silver"
GOLD_DIR = DATALAKE / "gold"
SILVER_CHANNELS = SILVER_DIR / "channels.parquet"
SILVER_VIDEOS = SILVER_DIR / "videos.parquet"


# --------------------------------------------------------------------------- #
# Cliente e utilitarios
# --------------------------------------------------------------------------- #
def get_youtube_client(api_key: str | None):
    """Constroi um cliente da YouTube Data API v3 (um por thread)."""
    if not api_key:
        raise ValueError("API_KEY nao encontrada no arquivo .env.")

    return build("youtube", "v3", developerKey=api_key)


def load_watchlists(config_path: Path = CONFIG_PATH) -> list[dict[str, Any]]:
    """Le o contrato de entrada e valida o limite de canais por cliente."""
    if not config_path.exists():
        raise FileNotFoundError(f"Arquivo de watchlist nao encontrado: {config_path}")

    with config_path.open(encoding="utf-8") as file:
        payload = json.load(file)

    clients = payload.get("clientes", [])
    for client in clients:
        channels = client.get("canais", [])
        if not 1 <= len(channels) <= MAX_CHANNELS_PER_CLIENT:
            raise ValueError(
                f"Cliente '{client.get('cliente')}' deve ter de 1 a "
                f"{MAX_CHANNELS_PER_CLIENT} canais (recebeu {len(channels)})."
            )
    return clients


def handle_slug(channel_handle: str) -> str:
    """Normaliza o handle para uso em nome de arquivo."""
    return channel_handle.lstrip("@").lower()


def chunked(values: list[str], batch_size: int) -> list[list[str]]:
    """Quebra uma lista em lotes de tamanho fixo."""
    return [values[i:i + batch_size] for i in range(0, len(values), batch_size)]


def parse_iso8601_duration(duration: str | None) -> int | None:
    """Converte uma duracao ISO-8601 (ex.: PT1H2M3S) em segundos."""
    if not duration:
        return None

    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        return None

    hours, minutes, seconds = (int(value or 0) for value in match.groups())
    return hours * 3600 + minutes * 60 + seconds


# --------------------------------------------------------------------------- #
# BRONZE -- extracao do snapshot bruto
# --------------------------------------------------------------------------- #
def collect_channel_bronze(task: dict[str, Any], ingested_at: str) -> dict[str, Any]:
    """Coleta 1 canal (thread) e grava o parquet bruto na bronze."""
    handle = task["channel_handle"]
    youtube = get_youtube_client(API_KEY)
    try:
        response = youtube.channels().list(
            part=CHANNEL_PARTS, forHandle=handle
        ).execute()
    except HttpError as exc:
        raise RuntimeError(f"Erro ao consultar o canal {handle}: {exc}") from exc

    items = response.get("items", [])
    if not items:
        raise ValueError(f"Nenhum canal encontrado para o handle {handle}.")

    item = items[0]
    snippet = item.get("snippet", {})
    statistics = item.get("statistics", {})
    related = item.get("contentDetails", {}).get("relatedPlaylists", {})

    record = {
        "client_name": task["client_name"],
        "is_owner": task["is_owner"],
        "channel_handle": handle,
        "channel_id": item.get("id"),
        "channel_title": snippet.get("title"),
        "channel_published_at": snippet.get("publishedAt"),
        "channel_country": snippet.get("country"),
        "view_count": statistics.get("viewCount"),
        "subscriber_count": statistics.get("subscriberCount"),
        "video_count": statistics.get("videoCount"),
        "uploads_playlist_id": related.get("uploads"),
        "ingested_at": ingested_at,
    }

    destination = (
        BRONZE_CHANNELS_DIR
        / f"{task['client_name']}__{handle_slug(handle)}.parquet"
    )
    pd.DataFrame([record]).to_parquet(destination, engine="fastparquet", index=False)
    return record


def collect_channel_videos_bronze(
    channel_record: dict[str, Any], ingested_at: str
) -> list[dict[str, Any]]:
    """Coleta os videos de 1 canal (thread) e grava o parquet bruto na bronze."""
    playlist_id = channel_record.get("uploads_playlist_id")
    if not playlist_id:
        return []

    youtube = get_youtube_client(API_KEY)

    playlist_items: list[dict[str, Any]] = []
    next_page_token: str | None = None
    for _ in range(MAX_PLAYLIST_PAGES):
        try:
            response = youtube.playlistItems().list(
                part=PLAYLIST_PARTS,
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            ).execute()
        except HttpError as exc:
            raise RuntimeError(f"Erro na playlist {playlist_id}: {exc}") from exc

        playlist_items.extend(response.get("items", []))
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    playlist_by_video_id = {
        item.get("contentDetails", {}).get("videoId"): item
        for item in playlist_items
        if item.get("contentDetails", {}).get("videoId")
    }

    video_items: list[dict[str, Any]] = []
    for batch in chunked(list(playlist_by_video_id.keys()), batch_size=50):
        try:
            response = youtube.videos().list(
                part=VIDEO_PARTS, id=",".join(batch), maxResults=50
            ).execute()
        except HttpError as exc:
            raise RuntimeError(f"Erro nos videos: {exc}") from exc
        video_items.extend(response.get("items", []))

    records: list[dict[str, Any]] = []
    for video_item in video_items:
        video_id = video_item.get("id")
        if video_id not in playlist_by_video_id:
            continue
        snippet = video_item.get("snippet", {})
        statistics = video_item.get("statistics", {})
        content_details = video_item.get("contentDetails", {})
        records.append({
            "client_name": channel_record["client_name"],
            "channel_id": channel_record["channel_id"],
            "channel_handle": channel_record["channel_handle"],
            "video_id": video_id,
            "video_title": snippet.get("title"),
            "video_published_at": snippet.get("publishedAt"),
            "video_duration": content_details.get("duration"),
            "video_definition": content_details.get("definition"),
            "view_count": statistics.get("viewCount"),
            "like_count": statistics.get("likeCount"),
            "favorite_count": statistics.get("favoriteCount"),
            "comment_count": statistics.get("commentCount"),
            "ingested_at": ingested_at,
        })

    if records:
        destination = (
            BRONZE_VIDEOS_DIR
            / f"{channel_record['client_name']}__{handle_slug(channel_record['channel_handle'])}.parquet"
        )
        pd.DataFrame(records).to_parquet(destination, engine="fastparquet", index=False)

    return records


def run_bronze(
    watchlists: list[dict[str, Any]], ingested_at: str
) -> list[dict[str, Any]]:
    """Extrai o snapshot bruto de todos os canais das watchlists, em paralelo."""
    tasks: list[dict[str, Any]] = []
    for client in watchlists:
        owner = client.get("meu_canal")
        for handle in client["canais"]:
            tasks.append({
                "client_name": client["cliente"],
                "channel_handle": handle,
                "is_owner": handle == owner,
            })

    channel_records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(collect_channel_bronze, task, ingested_at): task
            for task in tasks
        }
        for future in as_completed(futures):
            task = futures[future]
            try:
                channel_records.append(future.result())
            except (RuntimeError, ValueError) as exc:
                print(f"[canal falhou] {task['channel_handle']}: {exc}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(collect_channel_videos_bronze, record, ingested_at):
                record["channel_handle"]
            for record in channel_records
        }
        for future in as_completed(futures):
            handle = futures[future]
            try:
                count = len(future.result())
                print(f"  videos: {handle:20} -> {count}")
            except (RuntimeError, ValueError) as exc:
                print(f"[videos falhou] {handle}: {exc}")

    return channel_records


# --------------------------------------------------------------------------- #
# SILVER -- tratamento + acumulo por snapshot
# --------------------------------------------------------------------------- #
def read_bronze(directory: Path) -> pd.DataFrame:
    """Le e concatena todos os parquet brutos de uma pasta da bronze."""
    files = sorted(directory.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat(
        (pd.read_parquet(file, engine="fastparquet") for file in files),
        ignore_index=True,
    )


def accumulate(new_data: pd.DataFrame, target: Path, keys: list[str]) -> pd.DataFrame:
    """Faz append idempotente do snapshot ao historico (dedup por chave)."""
    frames = [new_data]
    if target.exists():
        frames.insert(0, pd.read_parquet(target, engine="fastparquet"))

    history = pd.concat(frames, ignore_index=True)
    history = history.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)

    target.parent.mkdir(parents=True, exist_ok=True)
    history.to_parquet(target, engine="fastparquet", index=False)
    return history


def build_silver_channels() -> pd.DataFrame:
    """Trata os canais e acumula por (channel_id, client, ingested_at)."""
    dataframe = read_bronze(BRONZE_CHANNELS_DIR)
    if dataframe.empty:
        return dataframe

    for column in ("view_count", "subscriber_count", "video_count"):
        dataframe[column] = pd.to_numeric(
            dataframe[column], errors="coerce"
        ).astype("Int64")

    dataframe["channel_published_at"] = pd.to_datetime(
        dataframe["channel_published_at"], errors="coerce", utc=True
    )
    dataframe["ingested_at"] = pd.to_datetime(
        dataframe["ingested_at"], errors="coerce", utc=True
    )
    dataframe["channel_country"] = (
        dataframe["channel_country"].fillna("UNKNOWN").str.upper()
    )
    dataframe["channel_title"] = dataframe["channel_title"].str.strip()

    return accumulate(
        dataframe, SILVER_CHANNELS,
        keys=["client_name", "channel_id", "ingested_at"],
    )


def build_silver_videos() -> pd.DataFrame:
    """Trata os videos (duracao em segundos) e acumula por snapshot."""
    dataframe = read_bronze(BRONZE_VIDEOS_DIR)
    if dataframe.empty:
        return dataframe

    for column in ("view_count", "like_count", "favorite_count", "comment_count"):
        dataframe[column] = pd.to_numeric(
            dataframe[column], errors="coerce"
        ).astype("Int64")

    dataframe["video_published_at"] = pd.to_datetime(
        dataframe["video_published_at"], errors="coerce", utc=True
    )
    dataframe["ingested_at"] = pd.to_datetime(
        dataframe["ingested_at"], errors="coerce", utc=True
    )
    dataframe["video_duration_seconds"] = (
        dataframe["video_duration"].map(parse_iso8601_duration).astype("Int64")
    )
    dataframe["video_title"] = dataframe["video_title"].str.strip()

    return accumulate(
        dataframe, SILVER_VIDEOS,
        keys=["client_name", "video_id", "ingested_at"],
    )


# --------------------------------------------------------------------------- #
# GOLD -- dimensoes, fatos e marts analiticos
# --------------------------------------------------------------------------- #
def _write_gold(dataframe: pd.DataFrame, name: str) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(GOLD_DIR / f"{name}.parquet", engine="fastparquet", index=False)


def _movement(rank: Any, prev_rank: Any) -> str:
    """Classifica a variacao de posicao no ranking."""
    if pd.isna(prev_rank):
        return "novo"
    if rank < prev_rank:
        return "subiu"
    if rank > prev_rank:
        return "desceu"
    return "estavel"


def _status_top5(is_top5: bool, was_top5: Any) -> str:
    """Classifica a transicao de entrada/saida do top 5."""
    if pd.isna(was_top5):
        return "novo"
    if is_top5 and not was_top5:
        return "entrou"
    if not is_top5 and was_top5:
        return "saiu"
    return "permaneceu" if is_top5 else "fora"


def build_gold(
    silver_channels: pd.DataFrame, silver_videos: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """Monta dimensoes, fatos (serie temporal) e marts com metricas derivadas."""
    tables: dict[str, pd.DataFrame] = {}

    # ---- Dimensao: cliente ----
    owners = (
        silver_channels[silver_channels["is_owner"]]
        .groupby("client_name")["channel_handle"].first()
    )
    clients = sorted(silver_channels["client_name"].unique())
    dim_client = pd.DataFrame({
        "client_id": range(1, len(clients) + 1),
        "client_name": clients,
    })
    dim_client["owner_handle"] = dim_client["client_name"].map(owners)
    tables["dim_client"] = dim_client
    client_key = dict(zip(dim_client["client_name"], dim_client["client_id"]))

    # ---- Dimensao: canal (atributos estaveis) ----
    dim_channel = (
        silver_channels[[
            "channel_id", "channel_handle", "channel_title",
            "channel_country", "channel_published_at",
        ]]
        .drop_duplicates(subset=["channel_id"]).reset_index(drop=True)
    )
    tables["dim_channel"] = dim_channel

    # ---- Dimensao: video ----
    if not silver_videos.empty:
        dim_video = (
            silver_videos[[
                "video_id", "channel_id", "video_title", "video_published_at",
                "video_duration", "video_duration_seconds", "video_definition",
            ]]
            .drop_duplicates(subset=["video_id"]).reset_index(drop=True)
        )
    else:
        dim_video = pd.DataFrame()
    tables["dim_video"] = dim_video

    # ---- Fato: estatisticas de canal (serie temporal + rank) ----
    fact_channel = silver_channels.copy()
    fact_channel["client_id"] = fact_channel["client_name"].map(client_key)
    fact_channel["rank"] = (
        fact_channel.groupby(["client_name", "ingested_at"])["subscriber_count"]
        .rank(method="first", ascending=False).astype("Int64")
    )
    fact_channel["is_top5"] = fact_channel["rank"] <= TOP_N
    fact_channel = fact_channel[[
        "client_id", "channel_id", "ingested_at", "subscriber_count",
        "view_count", "video_count", "rank", "is_top5", "is_owner",
    ]].reset_index(drop=True)
    tables["fact_channel_stats"] = fact_channel

    # ---- Fato: estatisticas de video (serie temporal) ----
    if not silver_videos.empty:
        fact_video = silver_videos.copy()
        fact_video["client_id"] = fact_video["client_name"].map(client_key)
        fact_video = fact_video[[
            "client_id", "channel_id", "video_id", "ingested_at",
            "view_count", "like_count", "favorite_count", "comment_count",
        ]].reset_index(drop=True)
    else:
        fact_video = pd.DataFrame()
    tables["fact_video_stats"] = fact_video

    # ---- Mart: crescimento + movimentacao de rank (entre snapshots) ----
    tables["mart_channel_growth"] = _build_growth_mart(fact_channel)

    # ---- Mart: foto do ultimo snapshot + engajamento + cadencia ----
    tables["mart_channel_snapshot"] = _build_snapshot_mart(
        fact_channel, dim_channel, silver_videos, client_key
    )

    for name, table in tables.items():
        _write_gold(table, name)
    return tables


def _build_growth_mart(fact_channel: pd.DataFrame) -> pd.DataFrame:
    """Calcula deltas de inscritos/views e a movimentacao de rank."""
    growth = fact_channel.sort_values(["channel_id", "ingested_at"]).copy()
    grouped = growth.groupby("channel_id", group_keys=False)

    growth["prev_subscriber_count"] = grouped["subscriber_count"].shift(1)
    growth["prev_view_count"] = grouped["view_count"].shift(1)
    growth["prev_rank"] = grouped["rank"].shift(1)

    growth["delta_subscribers"] = (
        growth["subscriber_count"] - growth["prev_subscriber_count"]
    )
    growth["delta_views"] = growth["view_count"] - growth["prev_view_count"]
    growth["rank_delta"] = growth["prev_rank"] - growth["rank"]  # + = subiu
    growth["was_top5"] = grouped["is_top5"].shift(1)

    growth["movement"] = [
        _movement(r, pr) for r, pr in zip(growth["rank"], growth["prev_rank"])
    ]
    growth["status_top5"] = [
        _status_top5(t, wt) for t, wt in zip(growth["is_top5"], growth["was_top5"])
    ]

    return growth[[
        "client_id", "channel_id", "ingested_at", "subscriber_count",
        "delta_subscribers", "view_count", "delta_views", "rank", "prev_rank",
        "rank_delta", "movement", "is_top5", "was_top5", "status_top5",
    ]].reset_index(drop=True)


def _build_snapshot_mart(
    fact_channel: pd.DataFrame,
    dim_channel: pd.DataFrame,
    silver_videos: pd.DataFrame,
    client_key: dict[str, int],
) -> pd.DataFrame:
    """Foto do ultimo snapshot com engajamento e cadencia de postagem."""
    latest_ts = fact_channel["ingested_at"].max()
    snapshot = fact_channel[fact_channel["ingested_at"] == latest_ts].copy()

    metrics = _channel_video_metrics(silver_videos, client_key, latest_ts)

    snapshot = snapshot.merge(
        dim_channel[["channel_id", "channel_title"]], on="channel_id", how="left"
    ).merge(metrics, on=["client_id", "channel_id"], how="left")

    return snapshot[[
        "client_id", "channel_id", "channel_title", "ingested_at", "rank",
        "is_top5", "is_owner", "subscriber_count", "view_count", "video_count",
        "engagement_rate", "videos_per_week",
    ]].sort_values(["client_id", "rank"]).reset_index(drop=True)


def _channel_video_metrics(
    silver_videos: pd.DataFrame,
    client_key: dict[str, int],
    latest_ts: Any,
) -> pd.DataFrame:
    """Engajamento (ponderado) e cadencia (videos/semana) do ultimo snapshot."""
    columns = ["client_id", "channel_id", "engagement_rate", "videos_per_week"]
    if silver_videos.empty:
        return pd.DataFrame(columns=columns)

    videos = silver_videos[silver_videos["ingested_at"] == latest_ts].copy()
    if videos.empty:
        return pd.DataFrame(columns=columns)

    videos["client_id"] = videos["client_name"].map(client_key)
    rows: list[dict[str, Any]] = []
    for (client_id, channel_id), group in videos.groupby(["client_id", "channel_id"]):
        views = group["view_count"].sum()
        interactions = group["like_count"].sum() + group["comment_count"].sum()
        engagement = float(interactions / views) if views else 0.0

        published = group["video_published_at"].dropna()
        span_days = (published.max() - published.min()).days if len(published) > 1 else 0
        weeks = max(span_days / 7, 1)
        videos_per_week = round(len(group) / weeks, 2)

        rows.append({
            "client_id": client_id,
            "channel_id": channel_id,
            "engagement_rate": round(engagement, 5),
            "videos_per_week": videos_per_week,
        })

    return pd.DataFrame(rows, columns=columns)


# --------------------------------------------------------------------------- #
# Orquestracao
# --------------------------------------------------------------------------- #
def prepare_bronze() -> None:
    """Recria a landing zone da bronze (snapshot atual). Silver/gold acumulam."""
    for directory in (BRONZE_CHANNELS_DIR, BRONZE_VIDEOS_DIR):
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True, exist_ok=True)


def run_pipeline() -> None:
    """Executa a pipeline completa para todas as watchlists configuradas."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    watchlists = load_watchlists()
    prepare_bronze()

    print(f">> BRONZE: coletando snapshot {ingested_at}...")
    run_bronze(watchlists, ingested_at)

    print(">> SILVER: tratando e acumulando historico...")
    silver_channels = build_silver_channels()
    silver_videos = build_silver_videos()

    print(">> GOLD: fatos, dimensoes e metricas derivadas...")
    gold = build_gold(silver_channels, silver_videos)

    _print_summary(gold)


def _print_summary(gold: dict[str, pd.DataFrame]) -> None:
    """Imprime a foto atual e a volumetria das tabelas."""
    snapshot = gold["mart_channel_snapshot"]
    print("\n=============== RANKING ATUAL (top 5 destacado) ===============")
    view = snapshot.copy()
    view["top5"] = view["is_top5"].map({True: "*", False: " "})
    print(view[[
        "rank", "top5", "channel_title", "subscriber_count",
        "engagement_rate", "videos_per_week", "is_owner",
    ]].to_string(index=False))

    n_snapshots = gold["fact_channel_stats"]["ingested_at"].nunique()
    print(f"\nSnapshots acumulados no historico: {n_snapshots}")
    print("\n=============== VOLUMETRIA ===============")
    for name, table in gold.items():
        print(f"gold.{name:22}: {len(table)} linhas")


# %%
if __name__ == "__main__":
    run_pipeline()
