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
"""Pipeline de monitoramento de uma watchlist definida pelo cliente.

O cliente informa ate 10 canais que deseja acompanhar (o front-end futuro
gravara essa lista em ``config/watchlists.json``). A cada execucao
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
    * observability : historico de execucoes e chamadas da API em Parquet, com
                      status, duracao, volumetria e erros.

Concorrencia (tema da branch): cada thread processa um unico canal, constroi o
proprio cliente (httplib2 nao e thread-safe) e grava em arquivo distinto -> sem
disputa pelo mesmo dado nem conflito de escrita.
"""

import argparse
import importlib.util
import json
import os
import re
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
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
OBSERVABILITY_DIR = DATALAKE / "observability"
PIPELINE_RUNS_PATH = OBSERVABILITY_DIR / "pipeline_runs.parquet"
API_CALLS_PATH = OBSERVABILITY_DIR / "api_calls.parquet"
OBSERVABILITY_DB_SCRIPT = Path(__file__).with_name("06_build_observability_db.py")


# --------------------------------------------------------------------------- #
# Observabilidade da execucao
# --------------------------------------------------------------------------- #
@dataclass
class PipelineMetrics:
    """Acumula metricas de uma execucao com seguranca entre threads."""

    run_id: str
    started_at: datetime
    ingested_at: str
    requested_channels: int = 0
    successful_channels: int = 0
    failed_channels: int = 0
    successful_video_channels: int = 0
    failed_video_channels: int = 0
    api_calls: int = 0
    api_errors: int = 0
    pipeline_errors: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    api_call_log: list[dict[str, Any]] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_channel_result(self, success: bool) -> None:
        with self._lock:
            if success:
                self.successful_channels += 1
            else:
                self.failed_channels += 1

    def record_video_result(self, success: bool) -> None:
        with self._lock:
            if success:
                self.successful_video_channels += 1
            else:
                self.failed_video_channels += 1

    def record_pipeline_error(self, stage: str, message: str) -> None:
        with self._lock:
            self.pipeline_errors += 1
            self.errors.append({
                "stage": stage,
                "error_type": "pipeline",
                "error_message": message[:2000],
            })

    def execute_api_request(
        self, request: Any, endpoint: str, resource: str
    ) -> Any:
        """Executa uma requisicao e registra duracao/status mesmo em caso de erro."""
        started = time.perf_counter()
        called_at = datetime.now(timezone.utc)
        with self._lock:
            self.api_calls += 1

        try:
            response = request.execute()
        except Exception as exc:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            status_code = getattr(getattr(exc, "resp", None), "status", None)
            event = {
                "run_id": self.run_id,
                "called_at": called_at.isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "endpoint": endpoint,
                "resource": resource,
                "status": "error",
                "duration_ms": duration_ms,
                "http_status": status_code,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:2000],
            }
            with self._lock:
                self.api_errors += 1
                self.api_call_log.append(event)
                self.errors.append({
                    "stage": endpoint,
                    "error_type": type(exc).__name__,
                    "error_message": f"{resource}: {str(exc)[:1900]}",
                })
            raise

        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        with self._lock:
            self.api_call_log.append({
                "run_id": self.run_id,
                "called_at": called_at.isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "endpoint": endpoint,
                "resource": resource,
                "status": "success",
                "duration_ms": duration_ms,
                "http_status": 200,
                "error_type": None,
                "error_message": None,
            })
        return response

    def build_run_record(
        self,
        finished_at: datetime,
        status: str,
        gold: dict[str, pd.DataFrame] | None = None,
        fatal_error: Exception | None = None,
    ) -> dict[str, Any]:
        """Monta uma linha resumida para ``pipeline_runs.parquet``."""
        with self._lock:
            errors = list(self.errors)
            error_count = self.pipeline_errors + self.api_errors

        latest_data_at: str | None = None
        silver_channel_rows = 0
        silver_video_rows = 0
        gold_rows = 0
        if gold:
            fact_channel = gold.get("fact_channel_stats", pd.DataFrame())
            fact_video = gold.get("fact_video_stats", pd.DataFrame())
            latest = fact_channel.get("ingested_at", pd.Series(dtype="datetime64[ns, UTC]")).max()
            latest_data_at = None if pd.isna(latest) else pd.Timestamp(latest).isoformat()
            silver_channel_rows = len(fact_channel)
            silver_video_rows = len(fact_video)
            gold_rows = sum(len(table) for table in gold.values())

        if fatal_error:
            errors.append({
                "stage": "pipeline",
                "error_type": type(fatal_error).__name__,
                "error_message": str(fatal_error)[:2000],
            })
            error_count += 1

        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "ingested_at": self.ingested_at,
            "status": status,
            "duration_seconds": round((finished_at - self.started_at).total_seconds(), 3),
            "requested_channels": self.requested_channels,
            "successful_channels": self.successful_channels,
            "failed_channels": self.failed_channels,
            "successful_video_channels": self.successful_video_channels,
            "failed_video_channels": self.failed_video_channels,
            "api_calls": self.api_calls,
            "api_errors": self.api_errors,
            "pipeline_errors": self.pipeline_errors,
            "total_errors": error_count,
            "error_message": " | ".join(
                str(item.get("error_message", "")) for item in errors
            )[:4000] or None,
            "data_latest_at": latest_data_at,
            "fact_channel_rows": silver_channel_rows,
            "fact_video_rows": silver_video_rows,
            "gold_rows": gold_rows,
        }


def _append_observability(
    run_record: dict[str, Any], api_call_log: list[dict[str, Any]]
) -> None:
    """Persiste resumo e chamadas sem apagar o historico anterior."""
    OBSERVABILITY_DIR.mkdir(parents=True, exist_ok=True)
    run_frame = pd.DataFrame([run_record])
    if PIPELINE_RUNS_PATH.exists():
        run_frame = pd.concat(
            [pd.read_parquet(PIPELINE_RUNS_PATH, engine="fastparquet"), run_frame],
            ignore_index=True,
        ).drop_duplicates("run_id", keep="last")
    run_frame.to_parquet(PIPELINE_RUNS_PATH, engine="fastparquet", index=False)

    if api_call_log:
        calls_frame = pd.DataFrame(api_call_log)
        if API_CALLS_PATH.exists():
            calls_frame = pd.concat(
                [pd.read_parquet(API_CALLS_PATH, engine="fastparquet"), calls_frame],
                ignore_index=True,
            )
        calls_frame.to_parquet(API_CALLS_PATH, engine="fastparquet", index=False)

    # O arquivo tem prefixo numerico por seguir a ordem didatica da pipeline;
    # por isso ele e carregado pelo caminho, e nao por um import convencional.
    spec = importlib.util.spec_from_file_location(
        "build_observability_db", OBSERVABILITY_DB_SCRIPT
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Nao foi possivel carregar {OBSERVABILITY_DB_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.build_database(run_record["finished_at"])


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
def collect_channel_bronze(
    task: dict[str, Any], ingested_at: str, metrics: PipelineMetrics | None = None
) -> dict[str, Any]:
    """Coleta 1 canal (thread) e grava o parquet bruto na bronze."""
    handle = task["channel_handle"]
    youtube = get_youtube_client(API_KEY)
    try:
        request = youtube.channels().list(part=CHANNEL_PARTS, forHandle=handle)
        response = (
            metrics.execute_api_request(request, "channels.list", handle)
            if metrics
            else request.execute()
        )
    except HttpError as exc:
        raise RuntimeError(f"Erro ao consultar o canal {handle}: {exc}") from exc
    except Exception as exc:
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
    channel_record: dict[str, Any],
    ingested_at: str,
    metrics: PipelineMetrics | None = None,
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
            request = youtube.playlistItems().list(
                part=PLAYLIST_PARTS,
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            response = (
                metrics.execute_api_request(request, "playlistItems.list", playlist_id)
                if metrics
                else request.execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"Erro na playlist {playlist_id}: {exc}") from exc
        except Exception as exc:
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
            request = youtube.videos().list(
                part=VIDEO_PARTS, id=",".join(batch), maxResults=50
            )
            response = (
                metrics.execute_api_request(request, "videos.list", channel_record["channel_id"])
                if metrics
                else request.execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"Erro nos videos: {exc}") from exc
        except Exception as exc:
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
    watchlists: list[dict[str, Any]],
    ingested_at: str,
    metrics: PipelineMetrics | None = None,
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

    if metrics:
        metrics.requested_channels = len(tasks)

    channel_records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(collect_channel_bronze, task, ingested_at, metrics): task
            for task in tasks
        }
        for future in as_completed(futures):
            task = futures[future]
            try:
                channel_records.append(future.result())
                if metrics:
                    metrics.record_channel_result(True)
            except (RuntimeError, ValueError) as exc:
                if metrics:
                    metrics.record_channel_result(False)
                    metrics.record_pipeline_error("bronze.channels", str(exc))
                print(f"[canal falhou] {task['channel_handle']}: {exc}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(collect_channel_videos_bronze, record, ingested_at, metrics):
                record["channel_handle"]
            for record in channel_records
        }
        for future in as_completed(futures):
            handle = futures[future]
            try:
                count = len(future.result())
                if metrics:
                    metrics.record_video_result(True)
                print(f"  videos: {handle:20} -> {count}")
            except (RuntimeError, ValueError) as exc:
                if metrics:
                    metrics.record_video_result(False)
                    metrics.record_pipeline_error("bronze.videos", str(exc))
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


def _canonicalize_channel_clients(
    silver_channels: pd.DataFrame, silver_videos: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Mantém o histórico quando o nome/cliente da watchlist é renomeado.

    ``client_name`` é um atributo da configuração e pode mudar. O ``channel_id``
    retornado pela API é estável, então o cliente mais recente associado a cada
    canal é aplicado também aos snapshots antigos antes de montar a gold.
    """
    if silver_channels.empty:
        return silver_channels, silver_videos

    latest_by_channel = (
        silver_channels.sort_values("ingested_at")
        .drop_duplicates(subset=["channel_id"], keep="last")
        .set_index("channel_id")
    )
    latest_client_by_channel = latest_by_channel["client_name"]
    latest_owner_by_channel = latest_by_channel["is_owner"]

    channels = silver_channels.copy()
    channels["client_name"] = (
        channels["channel_id"].map(latest_client_by_channel)
        .fillna(channels["client_name"])
    )
    channels["is_owner"] = (
        channels["channel_id"].map(latest_owner_by_channel)
        .fillna(channels["is_owner"])
    )
    channels = channels.drop_duplicates(
        subset=["client_name", "channel_id", "ingested_at"], keep="last"
    )

    videos = silver_videos.copy()
    if not videos.empty:
        videos["client_name"] = (
            videos["channel_id"].map(latest_client_by_channel)
            .fillna(videos["client_name"])
        )
        videos = videos.drop_duplicates(
            subset=["client_name", "channel_id", "video_id", "ingested_at"],
            keep="last",
        )
    return channels, videos


def build_gold(
    silver_channels: pd.DataFrame, silver_videos: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """Monta dimensoes, fatos (serie temporal) e marts com metricas derivadas."""
    # Permite trocar ``cliente``/``meu_canal`` sem romper a série histórica dos
    # canais que continuam na watchlist atual.
    silver_channels, silver_videos = _canonicalize_channel_clients(
        silver_channels, silver_videos
    )
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


def run_pipeline() -> dict[str, Any]:
    """Executa a pipeline e grava o resultado operacional em observability."""
    started_at = datetime.now(timezone.utc)
    ingested_at = started_at.isoformat()
    metrics = PipelineMetrics(
        run_id=f"{started_at.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}",
        started_at=started_at,
        ingested_at=ingested_at,
    )
    gold: dict[str, pd.DataFrame] | None = None
    fatal_error: Exception | None = None
    status = "error"

    try:
        watchlists = load_watchlists()
        prepare_bronze()

        print(f">> BRONZE: coletando snapshot {ingested_at}...")
        records = run_bronze(watchlists, ingested_at, metrics)
        if not records:
            raise RuntimeError("Nenhum canal foi coletado com sucesso.")

        print(">> SILVER: tratando e acumulando historico...")
        silver_channels = build_silver_channels()
        silver_videos = build_silver_videos()

        print(">> GOLD: fatos, dimensoes e metricas derivadas...")
        gold = build_gold(silver_channels, silver_videos)
        status = (
            "success"
            if metrics.failed_channels == 0
            and metrics.failed_video_channels == 0
            and metrics.api_errors == 0
            and metrics.pipeline_errors == 0
            else "partial"
        )
        _print_summary(gold)
    except Exception as exc:
        fatal_error = exc
        print(f"[pipeline falhou] {type(exc).__name__}: {exc}")
    finally:
        finished_at = datetime.now(timezone.utc)
        run_record = metrics.build_run_record(
            finished_at, status, gold=gold, fatal_error=fatal_error
        )
        try:
            _append_observability(run_record, metrics.api_call_log)
        except Exception as exc:
            # A falha ao registrar telemetria nao deve esconder a falha original.
            print(f"[observabilidade falhou] {type(exc).__name__}: {exc}")

    if fatal_error:
        raise fatal_error
    return run_record


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


def run_scheduler(interval_minutes: float) -> None:
    """Repete a pipeline em intervalo fixo, mantendo o processo vivo para testes."""
    if interval_minutes <= 0:
        raise ValueError("O intervalo deve ser maior que zero minutos.")

    interval_seconds = interval_minutes * 60
    print(f">> Modo agendado ativo: nova execução a cada {interval_minutes:g} minuto(s).")
    while True:
        cycle_started = time.perf_counter()
        try:
            run_pipeline()
        except Exception as exc:
            # A execução já foi registrada como error; o scheduler continua para
            # permitir que a próxima tentativa se recupere automaticamente.
            print(f"[scheduler] execução encerrada com erro: {type(exc).__name__}: {exc}")

        elapsed = time.perf_counter() - cycle_started
        wait_seconds = max(0.0, interval_seconds - elapsed)
        print(f">> Próxima execução em {wait_seconds / 60:.2f} minuto(s).")
        time.sleep(wait_seconds)


# %%
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa o pipeline de Channel Analytics.")
    parser.add_argument(
        "--interval-minutes",
        type=float,
        default=None,
        help="Repete a pipeline neste intervalo; omitido executa somente uma vez.",
    )
    args = parser.parse_args()
    configured_interval = os.getenv("PIPELINE_INTERVAL_MINUTES")

    interval_minutes = (
        float(configured_interval)
        if configured_interval
        else args.interval_minutes
    ) 
    if interval_minutes is None:
        run_pipeline()
    else:
        run_scheduler(interval_minutes)
