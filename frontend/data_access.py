"""Leitura e preparação das tabelas analíticas da camada gold."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


DEFAULT_GOLD_DIR = Path(__file__).resolve().parents[1] / "datalake" / "gold"
DEFAULT_OBSERVABILITY_DIR = Path(__file__).resolve().parents[1] / "datalake" / "observability"
REQUIRED_TABLES = {
    "dim_channel",
    "dim_client",
    "fact_channel_stats",
    "mart_channel_growth",
    "mart_channel_snapshot",
}


def gold_dir() -> Path:
    """Retorna o diretório gold configurado para execução local ou em Docker."""
    return Path(os.getenv("GOLD_DIR", DEFAULT_GOLD_DIR)).expanduser().resolve()


def observability_dir() -> Path:
    """Retorna o diretório dos artefatos operacionais da pipeline."""
    return Path(
        os.getenv("OBSERVABILITY_DIR", DEFAULT_OBSERVABILITY_DIR)
    ).expanduser().resolve()


def load_observability(
    path: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Carrega execuções e chamadas; retorna tabelas vazias antes da primeira run."""
    source = path or observability_dir()
    tables: dict[str, pd.DataFrame] = {}
    for name in ("pipeline_runs", "api_calls"):
        file = source / f"{name}.parquet"
        tables[name] = pd.read_parquet(file) if file.exists() else pd.DataFrame()
        if "ingested_at" in tables[name]:
            tables[name]["ingested_at"] = pd.to_datetime(
                tables[name]["ingested_at"], errors="coerce", utc=True
            )
        for column in ("started_at", "finished_at", "called_at"):
            if column in tables[name]:
                tables[name][column] = pd.to_datetime(
                    tables[name][column], errors="coerce", utc=True
                )
    return tables


def load_gold(path: Path | None = None) -> dict[str, pd.DataFrame]:
    """Carrega os Parquets gold e valida o contrato mínimo do dashboard."""
    source = path or gold_dir()
    if not source.exists():
        raise FileNotFoundError(f"Diretório da camada gold não encontrado: {source}")

    available = {file.stem for file in source.glob("*.parquet")}
    missing = REQUIRED_TABLES - available
    if missing:
        names = ", ".join(sorted(f"{name}.parquet" for name in missing))
        raise FileNotFoundError(f"Tabelas obrigatórias ausentes em {source}: {names}")

    tables = {
        name: pd.read_parquet(source / f"{name}.parquet")
        for name in sorted(available)
    }
    if tables["mart_channel_snapshot"].empty:
        raise ValueError("mart_channel_snapshot.parquet está vazio.")

    for table in tables.values():
        if "ingested_at" in table:
            table["ingested_at"] = pd.to_datetime(
                table["ingested_at"], errors="coerce", utc=True
            )
    if "dim_video" in tables and "video_published_at" in tables["dim_video"]:
        tables["dim_video"]["video_published_at"] = pd.to_datetime(
            tables["dim_video"]["video_published_at"], errors="coerce", utc=True
        )
    return tables


def channel_snapshot(tables: dict[str, pd.DataFrame], client_id: int) -> pd.DataFrame:
    """Enriquece a foto atual dos canais com dimensões e deltas do mesmo snapshot."""
    snapshot = tables["mart_channel_snapshot"]
    snapshot = snapshot[snapshot["client_id"] == client_id].copy()
    if snapshot.empty:
        return snapshot

    dimensions = tables["dim_channel"].drop_duplicates("channel_id")
    dimension_columns = [
        column
        for column in (
            "channel_id",
            "channel_handle",
            "channel_country",
            "channel_published_at",
        )
        if column in dimensions
    ]
    snapshot = snapshot.merge(dimensions[dimension_columns], on="channel_id", how="left")

    growth = tables["mart_channel_growth"]
    growth = growth[growth["client_id"] == client_id].copy()
    growth_columns = [
        "client_id",
        "channel_id",
        "ingested_at",
        "delta_subscribers",
        "delta_views",
        "rank_delta",
        "movement",
        "status_top5",
    ]
    growth_columns = [column for column in growth_columns if column in growth]
    snapshot = snapshot.merge(
        growth[growth_columns],
        on=["client_id", "channel_id", "ingested_at"],
        how="left",
    )
    return snapshot.sort_values("rank", na_position="last").reset_index(drop=True)


def channel_history(
    tables: dict[str, pd.DataFrame], client_id: int, channel_ids: list[str]
) -> pd.DataFrame:
    """Retorna a série histórica dos canais selecionados com seus nomes."""
    fact = tables["fact_channel_stats"]
    history = fact[
        (fact["client_id"] == client_id) & fact["channel_id"].isin(channel_ids)
    ].copy()
    names = tables["dim_channel"][["channel_id", "channel_title"]].drop_duplicates(
        "channel_id"
    )
    return history.merge(names, on="channel_id", how="left").sort_values("ingested_at")


def latest_videos(
    tables: dict[str, pd.DataFrame], client_id: int, channel_ids: list[str]
) -> pd.DataFrame:
    """Retorna o snapshot mais recente de vídeos e calcula engajamento individual."""
    fact = tables.get("fact_video_stats", pd.DataFrame())
    dimension = tables.get("dim_video", pd.DataFrame())
    if fact.empty or dimension.empty:
        return pd.DataFrame()

    videos = fact[
        (fact["client_id"] == client_id) & fact["channel_id"].isin(channel_ids)
    ].copy()
    if videos.empty:
        return videos

    latest = videos["ingested_at"].max()
    videos = videos[videos["ingested_at"] == latest]
    dimension_columns = [
        column
        for column in (
            "video_id",
            "video_title",
            "video_published_at",
            "video_duration_seconds",
            "video_definition",
        )
        if column in dimension
    ]
    videos = videos.merge(
        dimension[dimension_columns].drop_duplicates("video_id"),
        on="video_id",
        how="left",
    )
    channels = tables["dim_channel"][["channel_id", "channel_title"]].drop_duplicates(
        "channel_id"
    )
    videos = videos.merge(channels, on="channel_id", how="left")
    views = pd.to_numeric(videos["view_count"], errors="coerce").replace(0, pd.NA)
    interactions = videos[["like_count", "comment_count"]].fillna(0).sum(axis=1)
    videos["engagement_rate"] = interactions / views
    return videos.sort_values("view_count", ascending=False).reset_index(drop=True)
