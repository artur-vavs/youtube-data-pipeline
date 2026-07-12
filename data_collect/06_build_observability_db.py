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

A pipeline (04) chama esta sincronizacao automaticamente ao terminar. Este
script tambem pode ser executado sozinho para reconstruir o banco a partir dos
Parquets existentes e capturar a volumetria atual das camadas.
"""

import argparse
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

FORTALEZA = timezone(timedelta(hours=-3))  # UTC-3, sem horario de verao

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATALAKE = Path(os.getenv("DATALAKE_DIR", PROJECT_ROOT / "datalake"))
BRONZE_CHANNELS_DIR = DATALAKE / "bronze" / "channels"
BRONZE_VIDEOS_DIR = DATALAKE / "bronze" / "videos"
SILVER_DIR = DATALAKE / "silver"
GOLD_DIR = DATALAKE / "gold"
OBSERVABILITY_DIR = DATALAKE / "observability"
DB_PATH = OBSERVABILITY_DIR / "observability.db"


def folder_metrics(folder: Path) -> tuple[int, int]:
    """Conta arquivos e linhas de todos os parquet de uma pasta."""
    files = sorted(folder.glob("*.parquet"))
    rows = sum(len(pd.read_parquet(file, engine="fastparquet")) for file in files)
    return len(files), rows


def file_metrics(file: Path) -> tuple[int, int]:
    """Conta 1 arquivo e suas linhas (0 se nao existir)."""
    if not file.exists():
        return 0, 0
    return 1, len(pd.read_parquet(file, engine="fastparquet"))


def current_layer_metrics() -> list[tuple[str, str, int, int]]:
    """Mede a volumetria atual de cada camada: (layer, tabela, arquivos, linhas)."""
    metrics = [
        ("bronze", "channels", *folder_metrics(BRONZE_CHANNELS_DIR)),
        ("bronze", "videos", *folder_metrics(BRONZE_VIDEOS_DIR)),
        ("silver", "channels", *file_metrics(SILVER_DIR / "channels.parquet")),
        ("silver", "videos", *file_metrics(SILVER_DIR / "videos.parquet")),
    ]
    for file in sorted(GOLD_DIR.glob("*.parquet")):
        metrics.append(
            ("gold", file.stem, 1, len(pd.read_parquet(file, engine="fastparquet")))
        )
    return metrics


def previous_row_counts(conn: sqlite3.Connection) -> dict[tuple[str, str], int]:
    """Le a linha mais recente de cada camada/tabela ja registrada."""
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='layer_metrics'"
    ).fetchone()
    if not exists:
        return {}
    rows = conn.execute(
        """
        SELECT layer, table_name, row_count FROM (
            SELECT layer, table_name, row_count,
                   ROW_NUMBER() OVER (
                       PARTITION BY layer, table_name
                       ORDER BY datetime(captured_at) DESC, rowid DESC
                   ) AS position
            FROM layer_metrics
        )
        WHERE position = 1
        """
    ).fetchall()
    return {(layer, table_name): row_count for layer, table_name, row_count in rows}


def load_parquet_table(conn: sqlite3.Connection, name: str) -> int:
    """Carrega um parquet de observabilidade no SQLite (substitui o conteudo)."""
    file = OBSERVABILITY_DIR / f"{name}.parquet"
    if not file.exists():
        # Mantem o contrato do banco mesmo antes da primeira execucao.
        conn.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.execute(f'CREATE TABLE "{name}" (_empty INTEGER)')
        return 0
    frame = pd.read_parquet(file, engine="fastparquet")
    frame.to_sql(name, conn, if_exists="replace", index=False)
    return len(frame)


def build_database(captured_at: str | None = None) -> dict[str, int | str]:
    """Sincroniza os Parquets e registra uma captura idempotente das camadas.

    ``captured_at`` deve ser o instante da execucao da pipeline. Ao recebe-lo, uma
    nova tentativa da mesma execucao atualiza a captura em vez de duplica-la.
    """
    OBSERVABILITY_DIR.mkdir(parents=True, exist_ok=True)
    captured_at = captured_at or datetime.now(FORTALEZA).isoformat()

    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        runs = load_parquet_table(conn, "pipeline_runs")
        calls = load_parquet_table(conn, "api_calls")

        layer_metrics_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='layer_metrics'"
        ).fetchone()
        # Garante idempotencia caso a sincronizacao da mesma run seja repetida.
        if layer_metrics_exists:
            conn.execute(
                "DELETE FROM layer_metrics WHERE captured_at = ?",
                (captured_at,),
            )
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
        if records:
            pd.DataFrame(records).to_sql(
                "layer_metrics", conn, if_exists="append", index=False
            )

        if runs:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_id "
                "ON pipeline_runs(run_id)"
            )
        if calls:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_api_calls_run_id ON api_calls(run_id)"
            )
        if records:
            conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_layer_metrics_capture
                   ON layer_metrics(captured_at, layer, table_name)"""
            )

    print(f"OK: {DB_PATH}")
    print(f"  pipeline_runs : {runs} linhas")
    print(f"  api_calls     : {calls} linhas")
    print(f"  layer_metrics : +{len(records)} linhas (captura {captured_at})")
    return {
        "pipeline_runs": runs,
        "api_calls": calls,
        "layer_metrics": len(records),
        "captured_at": captured_at,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sincroniza a observabilidade em Parquet com o SQLite."
    )
    parser.add_argument(
        "--captured-at",
        help="Instante ISO da captura; omitido usa o horario atual de Fortaleza.",
    )
    args = parser.parse_args()
    build_database(args.captured_at)
