"""Dashboard Streamlit do Channel Analytics."""

from __future__ import annotations

from datetime import timezone

import altair as alt
import pandas as pd
import streamlit as st

from data_access import (
    channel_history,
    channel_snapshot,
    latest_videos,
    load_gold,
    load_observability,
)


st.set_page_config(
    page_title="Channel Analytics",
    page_icon="▶",
    layout="wide",
    initial_sidebar_state="expanded",
)

ACCENT = "#ff4b4b"
BLUE = "#2878d0"
MUTED = "#a8adb8"


def compact(value: object) -> str:
    if value is None or pd.isna(value):
        return "—"
    number = float(value)
    for suffix, divisor in ((" bi", 1_000_000_000), (" mi", 1_000_000), (" mil", 1_000)):
        if abs(number) >= divisor:
            return f"{number / divisor:.1f}{suffix}".replace(".0", "")
    return f"{number:,.0f}".replace(",", ".")


def percent(value: object) -> str:
    return "—" if value is None or pd.isna(value) else f"{float(value):.2%}"


def delta(value: object, *, inverse: bool = False) -> tuple[str | None, str]:
    if value is None or pd.isna(value):
        return None, "off"
    number = float(value)
    direction = "inverse" if inverse else "normal"
    return f"{number:+,.0f}".replace(",", "."), direction


def ranking_bar(data: pd.DataFrame, metric: str, title: str, selected_id: str) -> None:
    chart_data = data.dropna(subset=[metric]).sort_values(metric, ascending=True).copy()
    chart_data["destaque"] = chart_data["channel_id"].eq(selected_id)
    chart = (
        alt.Chart(chart_data)
        .mark_bar(cornerRadiusEnd=5)
        .encode(
            x=alt.X(f"{metric}:Q", title=title),
            y=alt.Y("channel_title:N", title=None, sort=None),
            color=alt.condition("datum.destaque", alt.value(ACCENT), alt.value(MUTED)),
            tooltip=[
                alt.Tooltip("channel_title:N", title="Canal"),
                alt.Tooltip(f"{metric}:Q", title=title, format=",.2f"),
            ],
        )
        .properties(height=max(260, len(chart_data) * 32))
    )
    st.altair_chart(chart, width="stretch")


@st.cache_data(ttl=60, show_spinner="Carregando camada gold…")
def cached_gold() -> dict[str, pd.DataFrame]:
    return load_gold()


@st.cache_data(ttl=60, show_spinner="Carregando observabilidade…")
def cached_observability() -> dict[str, pd.DataFrame]:
    return load_observability()


def render_header(snapshot_at: pd.Timestamp, client_name: str, channel_count: int) -> None:
    local_time = snapshot_at.tz_convert(timezone.utc).strftime("%d/%m/%Y às %H:%M UTC")
    st.markdown('<p class="eyebrow">YOUTUBE · WATCHLIST ANALYTICS</p>', unsafe_allow_html=True)
    st.title("Compare seu canal com quem disputa a atenção da sua audiência")
    st.caption(f"{client_name} · {channel_count} canais monitorados · snapshot de {local_time}")


def render_kpis(selected: pd.Series, total: int) -> None:
    cols = st.columns(5)
    subscriber_delta, subscriber_color = delta(selected.get("delta_subscribers"))
    view_delta, view_color = delta(selected.get("delta_views"))
    rank_delta, rank_color = delta(selected.get("rank_delta"))
    cols[0].metric("Inscritos", compact(selected.get("subscriber_count")), subscriber_delta, delta_color=subscriber_color)
    cols[1].metric("Visualizações", compact(selected.get("view_count")), view_delta, delta_color=view_color)
    cols[2].metric("Posição", f"#{int(selected['rank'])} de {total}", rank_delta, delta_color=rank_color)
    cols[3].metric("Engajamento", percent(selected.get("engagement_rate")))
    cadence = selected.get("videos_per_week")
    cols[4].metric("Cadência", "—" if pd.isna(cadence) else f"{cadence:.1f}/sem")


def overview_tab(snapshot: pd.DataFrame, selected_id: str) -> None:
    left, right = st.columns(2)
    with left:
        st.subheader("Ranking por inscritos")
        st.caption("O canal selecionado aparece em destaque.")
        ranking_bar(snapshot, "subscriber_count", "Inscritos", selected_id)
    with right:
        st.subheader("Taxa de engajamento")
        st.caption("(likes + comentários) ÷ visualizações dos vídeos coletados.")
        ranking_bar(snapshot, "engagement_rate", "Engajamento", selected_id)

    left, right = st.columns(2)
    with left:
        st.subheader("Cadência de publicação")
        st.caption("Quantidade estimada de vídeos publicados por semana.")
        ranking_bar(snapshot, "videos_per_week", "Vídeos por semana", selected_id)
    with right:
        st.subheader("Tamanho × engajamento")
        st.caption("Procure canais que engajam acima do esperado para seu tamanho.")
        scatter_data = snapshot.dropna(subset=["subscriber_count", "engagement_rate"]).copy()
        scatter_data["destaque"] = scatter_data["channel_id"].eq(selected_id)
        chart = (
            alt.Chart(scatter_data)
            .mark_circle(opacity=0.82, stroke="white", strokeWidth=1)
            .encode(
                x=alt.X("subscriber_count:Q", title="Inscritos", scale=alt.Scale(type="log")),
                y=alt.Y("engagement_rate:Q", title="Engajamento", axis=alt.Axis(format="%")),
                size=alt.Size("view_count:Q", title="Visualizações", scale=alt.Scale(range=[100, 1000])),
                color=alt.condition("datum.destaque", alt.value(ACCENT), alt.value(BLUE)),
                tooltip=[
                    alt.Tooltip("channel_title:N", title="Canal"),
                    alt.Tooltip("subscriber_count:Q", title="Inscritos", format=","),
                    alt.Tooltip("engagement_rate:Q", title="Engajamento", format=".2%"),
                    alt.Tooltip("videos_per_week:Q", title="Vídeos/sem", format=".1f"),
                ],
            )
            .properties(height=360)
        )
        st.altair_chart(chart, width="stretch")


def history_tab(history: pd.DataFrame) -> None:
    if history.empty:
        st.info("Ainda não há histórico para os canais selecionados.")
        return
    metric_labels = {
        "subscriber_count": "Inscritos",
        "view_count": "Visualizações",
        "rank": "Posição no ranking",
    }
    metric = st.segmented_control(
        "Métrica histórica",
        options=list(metric_labels),
        format_func=metric_labels.get,
        default="subscriber_count",
    )
    chart = (
        alt.Chart(history.dropna(subset=[metric]))
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X("ingested_at:T", title="Snapshot"),
            y=alt.Y(
                f"{metric}:Q",
                title=metric_labels[metric],
                scale=alt.Scale(reverse=metric == "rank", zero=False),
            ),
            color=alt.Color("channel_title:N", title="Canal"),
            tooltip=[
                alt.Tooltip("channel_title:N", title="Canal"),
                alt.Tooltip("ingested_at:T", title="Data", format="%d/%m/%Y %H:%M"),
                alt.Tooltip(f"{metric}:Q", title=metric_labels[metric], format=","),
            ],
        )
        .properties(height=440)
    )
    st.altair_chart(chart, width="stretch")
    if history["ingested_at"].nunique() < 2:
        st.caption("A série possui apenas um snapshot. Novos pontos aparecerão após as próximas execuções da pipeline.")


def videos_tab(videos: pd.DataFrame) -> None:
    if videos.empty:
        st.info("A camada gold ainda não possui fatos de vídeo para esta seleção.")
        return

    top_n = st.slider("Quantidade de vídeos no ranking", 5, min(30, len(videos)), min(10, len(videos)))
    chart_data = videos.head(top_n).sort_values("view_count", ascending=True)
    chart = (
        alt.Chart(chart_data)
        .mark_bar(cornerRadiusEnd=4, color=BLUE)
        .encode(
            x=alt.X("view_count:Q", title="Visualizações"),
            y=alt.Y("video_title:N", title=None, sort=None, axis=alt.Axis(labelLimit=330)),
            color=alt.Color("channel_title:N", title="Canal"),
            tooltip=[
                alt.Tooltip("video_title:N", title="Vídeo"),
                alt.Tooltip("channel_title:N", title="Canal"),
                alt.Tooltip("view_count:Q", title="Views", format=","),
                alt.Tooltip("like_count:Q", title="Likes", format=","),
                alt.Tooltip("engagement_rate:Q", title="Engajamento", format=".2%"),
            ],
        )
        .properties(height=max(300, top_n * 34))
    )
    st.subheader("Vídeos com mais visualizações")
    st.altair_chart(chart, width="stretch")

    st.subheader("Detalhes dos vídeos")
    table = videos[[
        "video_title", "channel_title", "video_published_at", "view_count",
        "like_count", "comment_count", "engagement_rate",
    ]].rename(columns={
        "video_title": "Vídeo", "channel_title": "Canal", "video_published_at": "Publicado em",
        "view_count": "Views", "like_count": "Likes", "comment_count": "Comentários",
        "engagement_rate": "Engajamento",
    })
    st.dataframe(
        table,
        width="stretch",
        hide_index=True,
        column_config={
            "Publicado em": st.column_config.DatetimeColumn(format="DD/MM/YYYY"),
            "Views": st.column_config.NumberColumn(format="compact"),
            "Likes": st.column_config.NumberColumn(format="compact"),
            "Comentários": st.column_config.NumberColumn(format="compact"),
            "Engajamento": st.column_config.NumberColumn(format="percent"),
        },
    )


def ranking_tab(snapshot: pd.DataFrame, selected_id: str) -> None:
    view = snapshot.copy()
    view["Canal"] = view["channel_title"] + view["is_owner"].map({True: " · seu canal", False: ""})
    view["Movimento"] = view["movement"].map({
        "subiu": "↑ Subiu", "desceu": "↓ Desceu", "estavel": "→ Estável", "novo": "● Novo"
    }).fillna("—")
    view["Top 5"] = view["status_top5"].map({
        "entrou": "Entrou", "saiu": "Saiu", "permaneceu": "Permaneceu", "fora": "Fora", "novo": "Novo"
    }).fillna("—")
    table = view[[
        "rank", "Canal", "subscriber_count", "delta_subscribers", "delta_views",
        "Movimento", "Top 5", "engagement_rate", "videos_per_week",
    ]].rename(columns={
        "rank": "Posição", "subscriber_count": "Inscritos", "delta_subscribers": "Δ inscritos",
        "delta_views": "Δ views", "engagement_rate": "Engajamento", "videos_per_week": "Vídeos/sem",
    })
    st.dataframe(
        table,
        width="stretch",
        hide_index=True,
        column_config={
            "Posição": st.column_config.NumberColumn(format="#%d"),
            "Inscritos": st.column_config.NumberColumn(format="compact"),
            "Δ inscritos": st.column_config.NumberColumn(format="%+d"),
            "Δ views": st.column_config.NumberColumn(format="%+d"),
            "Engajamento": st.column_config.NumberColumn(format="percent"),
            "Vídeos/sem": st.column_config.NumberColumn(format="%.1f"),
        },
    )

    selected = snapshot.loc[snapshot["channel_id"] == selected_id].iloc[0]
    leader = snapshot.dropna(subset=["rank"]).sort_values("rank").iloc[0]
    st.subheader("Leitura rápida")
    messages = []
    if selected_id == leader["channel_id"]:
        messages.append("O canal selecionado lidera a watchlist em inscritos.")
    else:
        gap = leader["subscriber_count"] - selected["subscriber_count"]
        messages.append(f"A distância para {leader['channel_title']}, líder em inscritos, é de {compact(gap)}.")
    engagement_median = snapshot["engagement_rate"].median()
    if pd.notna(selected.get("engagement_rate")) and pd.notna(engagement_median):
        position = "acima" if selected["engagement_rate"] >= engagement_median else "abaixo"
        messages.append(f"O engajamento está {position} da mediana da watchlist ({percent(engagement_median)}).")
    st.markdown("\n".join(f"- {message}" for message in messages))


def _display_timestamp(value: object) -> str:
    if value is None or pd.isna(value):
        return "—"
    return pd.Timestamp(value).strftime("%d/%m/%Y %H:%M:%S UTC")


def observability_tab(observability: dict[str, pd.DataFrame]) -> None:
    """Exibe saúde da última execução e detalhes das falhas da API."""
    runs = observability.get("pipeline_runs", pd.DataFrame()).copy()
    calls = observability.get("api_calls", pd.DataFrame()).copy()
    if runs.empty:
        st.info("Ainda não há execuções registradas. A telemetria aparecerá após a próxima execução da pipeline.")
        return

    runs = runs.sort_values("finished_at", na_position="first")
    latest = runs.iloc[-1]
    status_labels = {"success": "Sucesso", "partial": "Parcial", "error": "Erro"}
    status = status_labels.get(str(latest.get("status")), str(latest.get("status", "—")))
    status_delta = "normal" if latest.get("status") == "success" else "inverse"
    last_data = latest.get("data_latest_at")
    successful_runs = runs[runs["status"].isin(["success", "partial"])]
    if not successful_runs.empty and "data_latest_at" in successful_runs:
        latest_data = successful_runs["data_latest_at"].dropna()
        last_data = latest_data.iloc[-1] if not latest_data.empty else last_data

    cols = st.columns(5)
    cols[0].metric("Última execução", status, _display_timestamp(latest.get("finished_at")), delta_color=status_delta)
    cols[1].metric("Duração", f"{float(latest.get('duration_seconds', 0)):.1f}s")
    cols[2].metric("Chamadas API", f"{int(latest.get('api_calls', 0))}", f"{int(latest.get('api_errors', 0))} erros")
    cols[3].metric("Dados atualizados", _display_timestamp(last_data))
    cols[4].metric("Canais", f"{int(latest.get('successful_channels', 0))}/{int(latest.get('requested_channels', 0))}")

    if latest.get("status") == "success":
        st.success("A última execução terminou com sucesso e todos os canais foram processados.")
    elif latest.get("status") == "partial":
        st.warning("A última execução gerou dados, mas houve falhas recuperáveis em canais ou chamadas da API.")
    else:
        st.error("A última execução terminou com erro. O snapshot desta execução não deve ser considerado completo.")

    st.subheader("Histórico de execuções")
    run_columns = [
        "finished_at", "status", "duration_seconds", "requested_channels",
        "successful_channels", "failed_channels", "api_calls", "api_errors",
        "gold_rows", "error_message",
    ]
    run_columns = [column for column in run_columns if column in runs]
    run_view = runs.tail(20)[run_columns].sort_values("finished_at", ascending=False).rename(columns={
        "finished_at": "Finalizada em", "status": "Status", "duration_seconds": "Duração (s)",
        "requested_channels": "Canais solicitados", "successful_channels": "Canais OK",
        "failed_channels": "Canais com erro", "api_calls": "Chamadas API", "api_errors": "Erros API",
        "gold_rows": "Linhas gold", "error_message": "Resumo dos erros",
    })
    st.dataframe(
        run_view,
        width="stretch",
        hide_index=True,
        column_config={
            "Finalizada em": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm:ss"),
            "Duração (s)": st.column_config.NumberColumn(format="%.1f"),
            "Resumo dos erros": st.column_config.TextColumn(width="large"),
        },
    )

    latest_calls = calls[calls["run_id"] == latest["run_id"]] if not calls.empty else calls
    latest_errors = latest_calls[latest_calls["status"] == "error"] if not latest_calls.empty else latest_calls
    if not latest_errors.empty:
        st.subheader("Erros de chamadas na última execução")
        error_columns = [
            "called_at", "endpoint", "resource", "duration_ms", "http_status",
            "error_type", "error_message",
        ]
        error_columns = [column for column in error_columns if column in latest_errors]
        st.dataframe(
            latest_errors.sort_values("called_at", ascending=False)[error_columns].rename(columns={
                "called_at": "Iniciada em", "endpoint": "Endpoint", "resource": "Recurso",
                "duration_ms": "Duração (ms)", "http_status": "HTTP", "error_type": "Tipo",
                "error_message": "Erro",
            }),
            width="stretch",
            hide_index=True,
            column_config={
                "Iniciada em": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm:ss"),
                "Duração (ms)": st.column_config.NumberColumn(format="%.2f"),
                "Erro": st.column_config.TextColumn(width="large"),
            },
        )
    elif int(latest.get("api_errors", 0)) == 0:
        st.caption("Nenhum erro de chamada à API foi registrado na última execução.")

    with st.expander("Como interpretar os status"):
        st.markdown(
            "- **Sucesso**: todos os canais e chamadas foram processados.\n"
            "- **Parcial**: a pipeline concluiu e gerou dados, mas houve falhas recuperáveis.\n"
            "- **Erro**: houve falha fatal; a execução não foi considerada um snapshot completo."
        )


def main() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 2rem; padding-bottom: 4rem; max-width: 1440px;}
        .eyebrow {font-size:.74rem; letter-spacing:.12em; color:#ff4b4b; font-weight:700; margin-bottom:-.5rem;}
        [data-testid="stMetric"] {border:1px solid rgba(128,128,128,.20); border-radius:12px; padding:1rem;}
        [data-testid="stMetricValue"] {font-size:1.65rem;}
        div[data-testid="stSidebarContent"] {padding-top:1.2rem;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    observability = cached_observability()
    try:
        tables = cached_gold()
    except (FileNotFoundError, ValueError, OSError) as exc:
        st.error("Não foi possível carregar a camada gold.")
        st.code(str(exc))
        st.info("Execute a pipeline para gerar datalake/gold/*.parquet e recarregue a página.")
        observability_tab(observability)
        st.stop()

    clients = tables["dim_client"].sort_values("client_name")
    client_options = dict(zip(clients["client_name"], clients["client_id"]))
    with st.sidebar:
        st.header("Filtros")
        client_name = st.selectbox("Cliente / watchlist", list(client_options))
        client_id = int(client_options[client_name])
        snapshot = channel_snapshot(tables, client_id)
        if snapshot.empty:
            st.warning("Este cliente não possui snapshot na gold.")
            st.stop()

        owner_rows = snapshot[snapshot["is_owner"]]
        default_channel = owner_rows.iloc[0]["channel_id"] if not owner_rows.empty else snapshot.iloc[0]["channel_id"]
        title_by_id = dict(zip(snapshot["channel_id"], snapshot["channel_title"]))
        selected_id = st.selectbox(
            "Canal em destaque",
            list(title_by_id),
            index=list(title_by_id).index(default_channel),
            format_func=title_by_id.get,
        )
        default_comparison = list(snapshot.head(min(5, len(snapshot)))["channel_id"])
        if selected_id not in default_comparison:
            default_comparison = [selected_id, *default_comparison[:-1]]
        comparison_ids = st.multiselect(
            "Canais nos gráficos",
            list(title_by_id),
            default=default_comparison,
            format_func=title_by_id.get,
            max_selections=10,
        )
        if selected_id not in comparison_ids:
            comparison_ids = [selected_id, *comparison_ids]
        st.caption("Os arquivos Parquet são lidos diretamente de datalake/gold.")
        if st.button("Recarregar dados", width="stretch"):
            st.cache_data.clear()
            st.rerun()

    filtered = snapshot[snapshot["channel_id"].isin(comparison_ids)].copy()
    selected = snapshot.loc[snapshot["channel_id"] == selected_id].iloc[0]
    snapshot_at = snapshot["ingested_at"].max()
    render_header(snapshot_at, client_name, len(snapshot))
    render_kpis(selected, len(snapshot))

    overview, evolution, video_analysis, ranking, operations = st.tabs(
        ["Visão geral", "Evolução", "Vídeos", "Ranking e movimento", "Observabilidade"]
    )
    with overview:
        overview_tab(filtered, selected_id)
    with evolution:
        history_tab(channel_history(tables, client_id, comparison_ids))
    with video_analysis:
        videos_tab(latest_videos(tables, client_id, comparison_ids))
    with ranking:
        ranking_tab(snapshot, selected_id)
    with operations:
        observability_tab(observability)

    st.caption("Fonte: camada gold do datalake · Channel Analytics")


if __name__ == "__main__":
    main()
