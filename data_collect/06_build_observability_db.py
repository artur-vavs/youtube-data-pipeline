# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "fastparquet>=2026.5.0",
#     "pandas>=3.0.0",
# ]
# ///

"""Cria/atualiza um banco SQLite APENAS com dados de observabilidade.

O SQLite NAO guarda os dados de negocio (canais/videos) — apenas telemetria:

    * pipeline_runs : historico de execucoes da pipeline (vindo do Parquet).
    * api_calls     : historico de chamadas a API (vindo do Parquet).
    * layer_metrics : volumetria e crescimento (delta) de cada camada bronze,
                      silver e gold a cada captura.

Rode este script depois da pipeline (04) para tambem capturar a bronze, que e
uma landing zone sobrescrita a cada execucao.
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

FORTALEZA = timezone(timedelta(hours=-3))  # UTC-3, sem horario de verao

DATALAKE = Path("datalake")
BRONZE_CHANNELS_DIR = DATALAKE / "bronze" / "channels"
BRONZE_VIDEOS_DIR = DATALAKE / "bronze" / "videos"
SILVER_DIR = DATALAKE / "silver"
GOLD_DIR = DATALAKE / "gold"
OBSERVABILITY_DIR = DATALAKE / "observability"
DB_PATH = OBSERVABILITY_DIR / "observability.db"


def folder_metrics(folder: Path) -> tuple[int, int]:
    """Conta arquivos e linhas de todos os parquet de uma pasta."""
    files = sorted(folder.glob("*.parquet"))
    rows = sum(len(pd.read_parquet(file)) for file in files)
    return len(files), rows


def file_metrics(file: Path) -> tuple[int, int]:
    """Conta 1 arquivo e suas linhas (0 se nao existir)."""
    if not file.exists():
        return 0, 0
    return 1, len(pd.read_parquet(file))


def current_layer_metrics() -> list[tuple[str, str, int, int]]:
    """Mede a volumetria atual de cada camada: (layer, tabela, arquivos, linhas)."""
    metrics = [
        ("bronze", "channels", *folder_metrics(BRONZE_CHANNELS_DIR)),
        ("bronze", "videos", *folder_metrics(BRONZE_VIDEOS_DIR)),
        ("silver", "channels", *file_metrics(SILVER_DIR / "channels.parquet")),
        ("silver", "videos", *file_metrics(SILVER_DIR / "videos.parquet")),
    ]
    for file in sorted(GOLD_DIR.glob("*.parquet")):
        metrics.append(("gold", file.stem, 1, len(pd.read_parquet(file))))
    return metrics


def previous_row_counts(conn: sqlite3.Connection) -> dict[tuple[str, str], int]:
    """Le a linha mais recente de cada camada/tabela ja registrada."""
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE name='layer_metrics'", conn)
    if tables.empty:
        return {}
    history = pd.read_sql("SELECT * FROM layer_metrics", conn)
    latest = history.sort_values("captured_at").drop_duplicates(
        ["layer", "table_name"], keep="last"
    )
    return {(row.layer, row.table_name): row.row_count for row in latest.itertuples()}


def load_parquet_table(conn: sqlite3.Connection, name: str) -> int:
    """Carrega um parquet de observabilidade no SQLite (substitui o conteudo)."""
    file = OBSERVABILITY_DIR / f"{name}.parquet"
    if not file.exists():
        return 0
    frame = pd.read_parquet(file)
    frame.to_sql(name, conn, if_exists="replace", index=False)
    return len(frame)


def build_database() -> None:
    """Monta o SQLite de observabilidade a partir dos Parquet."""
    OBSERVABILITY_DIR.mkdir(parents=True, exist_ok=True)
    captured_at = datetime.now(FORTALEZA).isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        runs = load_parquet_table(conn, "pipeline_runs")
        calls = load_parquet_table(conn, "api_calls")

        previous = previous_row_counts(conn)
        records = []
        for layer, table_name, file_count, row_count in current_layer_metrics():
            last = previous.get((layer, table_name))
            records.append({
                "captured_at": captured_at,
                "layer": layer,
                "table_name": table_name,
                "file_count": file_count,
                "row_count": row_count,
                "row_delta": None if last is None else row_count - last,
            })
        pd.DataFrame(records).to_sql("layer_metrics", conn, if_exists="append", index=False)

    print(f"OK: {DB_PATH}")
    print(f"  pipeline_runs : {runs} linhas")
    print(f"  api_calls     : {calls} linhas")
    print(f"  layer_metrics : +{len(records)} linhas (captura {captured_at})")


if __name__ == "__main__":
    build_database()
