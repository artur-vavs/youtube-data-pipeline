"use client";

import type { Channel } from "@/lib/data";
import { formatCompact, formatPct, formatSigned } from "@/lib/format";

function DeltaLabel({ value, suffix }: { value: number | null; suffix: string }) {
  if (value == null) {
    return <span className="kpi-delta flat">sem histórico</span>;
  }
  const cls = value > 0 ? "up" : value < 0 ? "down" : "flat";
  const arrow = value > 0 ? "▲" : value < 0 ? "▼" : "■";
  return (
    <span className={`kpi-delta ${cls}`}>
      {arrow} {formatSigned(value)} {suffix}
    </span>
  );
}

export default function KpiTiles({
  channel,
  total,
}: {
  channel: Channel;
  total: number;
}) {
  const rankDeltaCls =
    channel.rank_delta == null || channel.rank_delta === 0
      ? "flat"
      : channel.rank_delta > 0
        ? "up"
        : "down";
  const rankArrow =
    channel.rank_delta == null || channel.rank_delta === 0
      ? "■"
      : channel.rank_delta > 0
        ? "▲"
        : "▼";

  return (
    <div className="grid grid-kpi">
      <div className="card">
        <p className="card-label">Inscritos</p>
        <p className="kpi-value">{formatCompact(channel.subscriber_count)}</p>
        <DeltaLabel value={channel.delta_subscribers} suffix="vs. anterior" />
      </div>

      <div className="card">
        <p className="card-label">Engajamento</p>
        <p className="kpi-value">{formatPct(channel.engagement_rate)}</p>
        <span className="kpi-delta flat">(likes + coment.) ÷ views</span>
      </div>

      <div className="card">
        <p className="card-label">Cadência</p>
        <p className="kpi-value">{channel.videos_per_week ?? "—"}</p>
        <span className="kpi-delta flat">vídeos / semana</span>
      </div>

      <div className="card">
        <p className="card-label">Posição</p>
        <p className="kpi-value">
          {channel.rank ?? "—"}
          <span style={{ fontSize: 16, color: "var(--muted)" }}> / {total}</span>
        </p>
        <span className={`kpi-delta ${rankDeltaCls}`}>
          {rankArrow}{" "}
          {channel.rank_delta == null
            ? "sem histórico"
            : channel.rank_delta === 0
              ? "estável"
              : `${Math.abs(channel.rank_delta)} posição(ões)`}
        </span>
      </div>
    </div>
  );
}
