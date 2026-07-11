# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "fastparquet>=2026.5.0",
#     "pandas>=3.0.0",
# ]
# ///

"""Exporta a camada gold para um bundle JSON consumido pelo front-end Next.js.

Le os marts/fatos/dimensoes em ``datalake/gold`` e grava
``frontend/data/gold.json`` com:
    * clients  : lista de clientes/watchlists
    * channels : foto atual por canal (rank, metricas, deltas, movimentacao)
    * series   : serie temporal de inscritos/views/rank por canal (para os
                 graficos de evolucao)
"""

import json
from pathlib import Path
from typing import Any

import pandas as pd

GOLD_DIR = Path("datalake/gold")
OUTPUT_PATH = Path("frontend/data/gold.json")


def to_int(value: Any) -> int | None:
    """Converte valores nullable/NumPy para int JSON-serializavel."""
    if pd.isna(value):
        return None
    return int(value)


def to_float(value: Any) -> float | None:
    """Converte valores nullable/NumPy para float JSON-serializavel."""
    if pd.isna(value):
        return None
    return float(value)


def to_iso(value: Any) -> str | None:
    """Converte timestamp para ISO-8601."""
    if pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def build_bundle() -> dict[str, Any]:
    """Monta o dicionario final a partir das tabelas gold."""
    snapshot = pd.read_parquet(GOLD_DIR / "mart_channel_snapshot.parquet")
    growth = pd.read_parquet(GOLD_DIR / "mart_channel_growth.parquet")
    dim_channel = pd.read_parquet(GOLD_DIR / "dim_channel.parquet")
    dim_client = pd.read_parquet(GOLD_DIR / "dim_client.parquet")
    fact_channel = pd.read_parquet(GOLD_DIR / "fact_channel_stats.parquet")

    latest_ts = fact_channel["ingested_at"].max()
    growth_latest = growth[growth["ingested_at"] == latest_ts]

    channels = (
        snapshot
        .merge(
            dim_channel[[
                "channel_id", "channel_handle", "channel_country",
                "channel_published_at",
            ]],
            on="channel_id", how="left",
        )
        .merge(
            growth_latest[[
                "channel_id", "delta_subscribers", "delta_views",
                "rank_delta", "movement", "status_top5",
            ]],
            on="channel_id", how="left",
        )
    )

    clients = [
        {
            "client_id": to_int(row["client_id"]),
            "client_name": row["client_name"],
            "owner_handle": row["owner_handle"],
        }
        for _, row in dim_client.iterrows()
    ]

    channel_records = [
        {
            "channel_id": row["channel_id"],
            "client_id": to_int(row["client_id"]),
            "title": row["channel_title"],
            "handle": row["channel_handle"],
            "country": row["channel_country"],
            "published_at": to_iso(row["channel_published_at"]),
            "rank": to_int(row["rank"]),
            "is_top5": bool(row["is_top5"]),
            "is_owner": bool(row["is_owner"]),
            "subscriber_count": to_int(row["subscriber_count"]),
            "view_count": to_int(row["view_count"]),
            "video_count": to_int(row["video_count"]),
            "engagement_rate": to_float(row["engagement_rate"]),
            "videos_per_week": to_float(row["videos_per_week"]),
            "delta_subscribers": to_int(row["delta_subscribers"]),
            "delta_views": to_int(row["delta_views"]),
            "rank_delta": to_int(row["rank_delta"]),
            "movement": None if pd.isna(row["movement"]) else row["movement"],
            "status_top5": None if pd.isna(row["status_top5"]) else row["status_top5"],
        }
        for _, row in channels.iterrows()
    ]

    snapshots = sorted(to_iso(ts) for ts in fact_channel["ingested_at"].unique())
    series_by_channel: dict[str, list[dict[str, Any]]] = {}
    for channel_id, group in fact_channel.sort_values("ingested_at").groupby("channel_id"):
        series_by_channel[channel_id] = [
            {
                "ingested_at": to_iso(point["ingested_at"]),
                "subscriber_count": to_int(point["subscriber_count"]),
                "view_count": to_int(point["view_count"]),
                "rank": to_int(point["rank"]),
            }
            for _, point in group.iterrows()
        ]

    return {
        "generated_at": to_iso(latest_ts),
        "clients": clients,
        "channels": channel_records,
        "series": {"snapshots": snapshots, "by_channel": series_by_channel},
    }


if __name__ == "__main__":
    bundle = build_bundle()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as file:
        json.dump(bundle, file, ensure_ascii=False, indent=2)

    print(f"OK: {OUTPUT_PATH} gerado.")
    print(f"  clientes : {len(bundle['clients'])}")
    print(f"  canais   : {len(bundle['channels'])}")
    print(f"  snapshots: {len(bundle['series']['snapshots'])}")
