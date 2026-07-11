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
"""Pipeline medallion (bronze -> silver -> gold) do top 5 de canais por segmento.

Para cada segmento pre-definido, coletamos os canais candidatos (via handle),
selecionamos os 5 com mais inscritos e, para esses, coletamos os videos da
playlist de uploads (mesma logica do 01_data_collection_channel.py).

Camadas:
    * bronze : dados brutos, como retornados pela API (sem tratamento/tipagem).
               Cada canal e cada canal-de-videos gera um arquivo parquet proprio.
    * silver : dados tratados (tipagem, limpeza, deduplicacao) e o recorte de
               negocio (top 5 por segmento) aplicado.
    * gold   : modelagem dimensional (star schema) separando fatos e dimensoes.

Concorrencia (tema da branch):
    * Cada thread processa um unico item (um canal), portanto duas threads nunca
      coletam o mesmo dado -- nao ha disputa pelo mesmo recurso.
    * O cliente googleapiclient usa httplib2, que NAO e thread-safe; por isso
      cada thread constroi o proprio cliente (isolamento total de estado).
    * Cada thread grava em um arquivo parquet distinto -> sem conflito de escrita.
"""

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
MAX_PLAYLIST_PAGES = 2  # ~100 videos mais recentes por canal

DATALAKE = Path("datalake")
BRONZE_CHANNELS_DIR = DATALAKE / "bronze" / "channels"
BRONZE_VIDEOS_DIR = DATALAKE / "bronze" / "videos"
SILVER_DIR = DATALAKE / "silver"
GOLD_DIR = DATALAKE / "gold"

# Canais candidatos por segmento (handles validados na API).
# O ranking seleciona o TOP 5 por numero de inscritos dentro de cada segmento.
SEGMENTS: dict[str, list[str]] = {
    "financas_mercado": [
        "@MePoupe", "@GrahamStephan", "@AndreiJikh", "@EconoMirna",
        "@BTGPactual", "@InfoMoney", "@gustavocerbasi",
    ],
    "esportes_futebol": [
        "@CazeTV", "@ESPN", "@Desimpedidos", "@Flamengo", "@ESPNBrasil",
        "@brasileirao", "@DAZNFutebol",
    ],
    "geopolitica_noticias": [
        "@BBCNews", "@Vox", "@ViceNews", "@jovempannews", "@cnnbrasil",
        "@UOL", "@Reuters", "@meteorobrasil", "@estadao",
    ],
    "tecnologia_ia": [
        "@mkbhd", "@ManualdoMundo", "@LinusTechTips", "@ColdFusion",
        "@lexfridman", "@Fireship", "@TecMundo", "@Nerdologia",
        "@TwoMinutePapers", "@sentdex", "@tecnoblog",
    ],
    "marketing_digital": [
        "@GaryVee", "@NeilPatel", "@Hotmart", "@SocialMediaExaminer",
        "@semrush", "@resultadosdigitais",
    ],
    "saude_publica": [
        "@DoctorMike", "@DrauzioVarella", "@osmosis", "@medcram", "@WHO",
        "@nihgov", "@Fiocruz",
    ],
    "entretenimento": [
        "@MrBeast", "@tseries", "@PewDiePie", "@kondzilla", "@FelipeNeto",
        "@portadosfundos", "@cocielo",
    ],
    "ecommerce_reviews": [
        "@UnboxTherapy", "@Mrwhosetheboss", "@coisadenerd", "@Dave2D",
        "@tudocelular", "@AllThingsTech",
    ],
    "educacao_edtech": [
        "@TED", "@Kurzgesagt", "@veritasium", "@crashcourse", "@khanacademy",
        "@scishow", "@ProfessorFerretto", "@QueroBolsa", "@DescomplicaOficial",
    ],
    "seguranca_publica": [
        "@recordnews", "@brasilurgente", "@pmesp", "@DisqueDenuncia",
        "@jornaldacidadeonline", "@cidadealerta",
    ],
}


# --------------------------------------------------------------------------- #
# Cliente e utilitarios
# --------------------------------------------------------------------------- #
def get_youtube_client(api_key: str | None):
    """Constroi um cliente da YouTube Data API v3 (um por thread)."""
    if not api_key:
        raise ValueError("API_KEY nao encontrada no arquivo .env.")

    return build("youtube", "v3", developerKey=api_key)


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

    match = re.fullmatch(
        r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration
    )
    if not match:
        return None

    hours, minutes, seconds = (int(value or 0) for value in match.groups())
    return hours * 3600 + minutes * 60 + seconds


# --------------------------------------------------------------------------- #
# BRONZE -- extracao de dados brutos
# --------------------------------------------------------------------------- #
def raw_channel_record(
    channel_item: dict[str, Any],
    channel_handle: str,
    segment: str,
    ingested_at: str,
) -> dict[str, Any]:
    """Achata o item de canal SEM tratamento (tudo como veio da API)."""
    snippet = channel_item.get("snippet", {})
    statistics = channel_item.get("statistics", {})
    content_details = channel_item.get("contentDetails", {})
    related_playlists = content_details.get("relatedPlaylists", {})

    return {
        "segment": segment,
        "channel_handle": channel_handle,
        "channel_id": channel_item.get("id"),
        "channel_title": snippet.get("title"),
        "channel_published_at": snippet.get("publishedAt"),
        "channel_country": snippet.get("country"),
        "view_count": statistics.get("viewCount"),
        "subscriber_count": statistics.get("subscriberCount"),
        "video_count": statistics.get("videoCount"),
        "uploads_playlist_id": related_playlists.get("uploads"),
        "ingested_at": ingested_at,
    }


def collect_channel_bronze(
    segment: str,
    channel_handle: str,
    ingested_at: str,
) -> dict[str, Any]:
    """Coleta 1 canal (thread) e grava o parquet bruto na bronze."""
    youtube = get_youtube_client(API_KEY)
    try:
        response = youtube.channels().list(
            part=CHANNEL_PARTS, forHandle=channel_handle
        ).execute()
    except HttpError as exc:
        raise RuntimeError(f"Erro ao consultar o canal {channel_handle}: {exc}") from exc

    items = response.get("items", [])
    if not items:
        raise ValueError(f"Nenhum canal encontrado para o handle {channel_handle}.")

    record = raw_channel_record(items[0], channel_handle, segment, ingested_at)

    destination = BRONZE_CHANNELS_DIR / f"{segment}__{handle_slug(channel_handle)}.parquet"
    pd.DataFrame([record]).to_parquet(destination, engine="fastparquet", index=False)

    return record


def collect_all_channels_bronze(
    segments: dict[str, list[str]],
    ingested_at: str,
) -> list[dict[str, Any]]:
    """Coleta os dados brutos de todos os candidatos, em paralelo."""
    tasks = [
        (segment, handle)
        for segment, handles in segments.items()
        for handle in handles
    ]

    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(collect_channel_bronze, segment, handle, ingested_at):
                (segment, handle)
            for segment, handle in tasks
        }
        for future in as_completed(futures):
            segment, handle = futures[future]
            try:
                records.append(future.result())
            except (RuntimeError, ValueError) as exc:
                print(f"[canal falhou] {segment}/{handle}: {exc}")

    return records


def raw_video_records(
    video_items: list[dict[str, Any]],
    playlist_by_video_id: dict[str, dict[str, Any]],
    channel_record: dict[str, Any],
    ingested_at: str,
) -> list[dict[str, Any]]:
    """Achata os videos SEM tratamento, ligando-os ao canal de origem."""
    records: list[dict[str, Any]] = []
    for video_item in video_items:
        video_id = video_item.get("id")
        if video_id not in playlist_by_video_id:
            continue

        playlist_item = playlist_by_video_id[video_id]
        snippet = video_item.get("snippet", {})
        statistics = video_item.get("statistics", {})
        content_details = video_item.get("contentDetails", {})

        records.append({
            "segment": channel_record["segment"],
            "channel_id": channel_record["channel_id"],
            "channel_handle": channel_record["channel_handle"],
            "channel_title": channel_record["channel_title"],
            "video_id": video_id,
            "video_title": snippet.get("title"),
            "video_published_at": snippet.get("publishedAt"),
            "playlist_item_position": playlist_item.get("snippet", {}).get("position"),
            "video_duration": content_details.get("duration"),
            "video_definition": content_details.get("definition"),
            "video_caption": content_details.get("caption"),
            "view_count": statistics.get("viewCount"),
            "like_count": statistics.get("likeCount"),
            "favorite_count": statistics.get("favoriteCount"),
            "comment_count": statistics.get("commentCount"),
            "ingested_at": ingested_at,
        })

    return records


def collect_channel_videos_bronze(
    channel_record: dict[str, Any],
    ingested_at: str,
) -> list[dict[str, Any]]:
    """Coleta os videos de 1 canal (thread) e grava o parquet bruto na bronze."""
    playlist_id = channel_record.get("uploads_playlist_id")
    if not playlist_id:
        return []

    youtube = get_youtube_client(API_KEY)

    # 1) Itens da playlist de uploads (paginado, com limite de paginas).
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

    # 2) Detalhes/estatisticas dos videos (em lotes de 50).
    video_items: list[dict[str, Any]] = []
    for batch in chunked(list(playlist_by_video_id.keys()), batch_size=50):
        try:
            response = youtube.videos().list(
                part=VIDEO_PARTS, id=",".join(batch), maxResults=50
            ).execute()
        except HttpError as exc:
            raise RuntimeError(f"Erro nos videos: {exc}") from exc
        video_items.extend(response.get("items", []))

    records = raw_video_records(
        video_items, playlist_by_video_id, channel_record, ingested_at
    )

    if records:
        destination = BRONZE_VIDEOS_DIR / f"{handle_slug(channel_record['channel_handle'])}.parquet"
        pd.DataFrame(records).to_parquet(destination, engine="fastparquet", index=False)

    return records


def collect_all_videos_bronze(
    top_channels: list[dict[str, Any]],
    ingested_at: str,
) -> None:
    """Coleta em paralelo os videos dos canais do top 5 de cada segmento."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(collect_channel_videos_bronze, channel, ingested_at):
                channel["channel_handle"]
            for channel in top_channels
        }
        for future in as_completed(futures):
            handle = futures[future]
            try:
                count = len(future.result())
                print(f"  videos coletados: {handle:22} -> {count}")
            except (RuntimeError, ValueError) as exc:
                print(f"[videos falhou] {handle}: {exc}")


# --------------------------------------------------------------------------- #
# Ranking -- recorte de negocio (top 5 por segmento)
# --------------------------------------------------------------------------- #
def rank_top_channels(
    channel_records: list[dict[str, Any]],
    top_n: int = TOP_N,
) -> pd.DataFrame:
    """Ranqueia e seleciona o top N de canais por inscritos em cada segmento."""
    dataframe = pd.DataFrame(channel_records)
    dataframe["subscriber_count"] = pd.to_numeric(
        dataframe["subscriber_count"], errors="coerce"
    ).astype("Int64")

    dataframe["rank"] = (
        dataframe.groupby("segment")["subscriber_count"]
        .rank(method="first", ascending=False)
        .astype("Int64")
    )

    top = dataframe[dataframe["rank"] <= top_n].copy()
    return top.sort_values(["segment", "rank"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# SILVER -- tratamento dos dados
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


def build_silver_channels(top_handles: set[str]) -> pd.DataFrame:
    """Trata os canais: tipagem, limpeza, dedup e recorte do top 5."""
    dataframe = read_bronze(BRONZE_CHANNELS_DIR)
    if dataframe.empty:
        return dataframe

    # Recorte de negocio: apenas os canais do top 5 por segmento.
    dataframe = dataframe[dataframe["channel_handle"].isin(top_handles)].copy()

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

    dataframe = dataframe.drop_duplicates(
        subset=["channel_id", "ingested_at"], keep="last"
    ).reset_index(drop=True)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(
        SILVER_DIR / "channels.parquet", engine="fastparquet", index=False
    )
    return dataframe


def build_silver_videos() -> pd.DataFrame:
    """Trata os videos: tipagem, duracao em segundos, limpeza e dedup."""
    dataframe = read_bronze(BRONZE_VIDEOS_DIR)
    if dataframe.empty:
        return dataframe

    numeric_columns = (
        "playlist_item_position", "view_count", "like_count",
        "favorite_count", "comment_count",
    )
    for column in numeric_columns:
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
    dataframe["video_caption"] = dataframe["video_caption"].map(
        {"true": True, "false": False}
    ).astype("boolean")

    dataframe = dataframe.drop_duplicates(
        subset=["video_id", "ingested_at"], keep="last"
    ).reset_index(drop=True)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(
        SILVER_DIR / "videos.parquet", engine="fastparquet", index=False
    )
    return dataframe


# --------------------------------------------------------------------------- #
# GOLD -- modelagem dimensional (fatos e dimensoes)
# --------------------------------------------------------------------------- #
def _write_gold(dataframe: pd.DataFrame, name: str) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(GOLD_DIR / f"{name}.parquet", engine="fastparquet", index=False)


def build_gold(
    silver_channels: pd.DataFrame,
    silver_videos: pd.DataFrame,
    ranking: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Monta o star schema: dim_segment, dim_channel, dim_video e as fatos."""
    tables: dict[str, pd.DataFrame] = {}

    # Dimensao: segmento
    segments = sorted(silver_channels["segment"].unique())
    dim_segment = pd.DataFrame({
        "segment_id": range(1, len(segments) + 1),
        "segment_name": segments,
    })
    tables["dim_segment"] = dim_segment
    segment_key = dict(zip(dim_segment["segment_name"], dim_segment["segment_id"]))

    # Dimensao: canal (atributos descritivos)
    dim_channel = silver_channels[[
        "channel_id", "channel_handle", "channel_title",
        "channel_country", "channel_published_at", "segment",
    ]].copy()
    dim_channel["segment_id"] = dim_channel["segment"].map(segment_key)
    dim_channel = dim_channel.drop(columns=["segment"]).drop_duplicates(
        subset=["channel_id"]
    ).reset_index(drop=True)
    tables["dim_channel"] = dim_channel

    # Dimensao: video (atributos descritivos)
    if not silver_videos.empty:
        dim_video = silver_videos[[
            "video_id", "channel_id", "video_title", "video_published_at",
            "video_duration", "video_duration_seconds", "video_definition",
            "video_caption",
        ]].drop_duplicates(subset=["video_id"]).reset_index(drop=True)
    else:
        dim_video = pd.DataFrame()
    tables["dim_video"] = dim_video

    # Fato: estatisticas de canal (metricas + rank + snapshot temporal)
    rank_key = ranking.set_index("channel_handle")["rank"]
    fact_channel_stats = silver_channels[[
        "channel_id", "segment", "view_count", "subscriber_count",
        "video_count", "ingested_at", "channel_handle",
    ]].copy()
    fact_channel_stats["segment_id"] = fact_channel_stats["segment"].map(segment_key)
    fact_channel_stats["rank"] = fact_channel_stats["channel_handle"].map(rank_key)
    fact_channel_stats = fact_channel_stats.drop(columns=["segment", "channel_handle"])
    tables["fact_channel_stats"] = fact_channel_stats.reset_index(drop=True)

    # Fato: estatisticas de video (metricas + snapshot temporal)
    if not silver_videos.empty:
        fact_video_stats = silver_videos[[
            "video_id", "channel_id", "view_count", "like_count",
            "favorite_count", "comment_count", "ingested_at",
        ]].copy()
    else:
        fact_video_stats = pd.DataFrame()
    tables["fact_video_stats"] = fact_video_stats.reset_index(drop=True)

    for name, table in tables.items():
        _write_gold(table, name)

    return tables


# --------------------------------------------------------------------------- #
# Orquestracao
# --------------------------------------------------------------------------- #
def reset_layers() -> None:
    """Recria as pastas de bronze/silver/gold para uma carga limpa."""
    for directory in (BRONZE_CHANNELS_DIR, BRONZE_VIDEOS_DIR, SILVER_DIR, GOLD_DIR):
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True, exist_ok=True)


def run_pipeline() -> None:
    """Executa a pipeline completa: bronze -> silver -> gold."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    reset_layers()

    print(">> BRONZE: coletando canais candidatos...")
    channel_records = collect_all_channels_bronze(SEGMENTS, ingested_at)

    print(">> Ranking: selecionando top 5 por segmento...")
    ranking = rank_top_channels(channel_records, TOP_N)
    top_handles = set(ranking["channel_handle"])
    top_channels = ranking.to_dict("records")

    print(">> BRONZE: coletando videos do top 5...")
    collect_all_videos_bronze(top_channels, ingested_at)

    print(">> SILVER: tratando canais e videos...")
    silver_channels = build_silver_channels(top_handles)
    silver_videos = build_silver_videos()

    print(">> GOLD: construindo fatos e dimensoes...")
    gold = build_gold(silver_channels, silver_videos, ranking)

    _print_summary(ranking, silver_channels, silver_videos, gold)


def _print_summary(
    ranking: pd.DataFrame,
    silver_channels: pd.DataFrame,
    silver_videos: pd.DataFrame,
    gold: dict[str, pd.DataFrame],
) -> None:
    """Imprime um resumo de validacao das camadas."""
    print("\n================= TOP 5 POR SEGMENTO =================")
    for segment, group in ranking.groupby("segment"):
        print(f"\n[{segment}]")
        view = group[["rank", "channel_title", "subscriber_count"]]
        print(view.to_string(index=False))

    print("\n================= VOLUMETRIA DAS CAMADAS =================")
    print(f"silver.channels : {len(silver_channels)} linhas")
    print(f"silver.videos   : {len(silver_videos)} linhas")
    for name, table in gold.items():
        print(f"gold.{name:20}: {len(table)} linhas")


# %%
if __name__ == "__main__":
    run_pipeline()
