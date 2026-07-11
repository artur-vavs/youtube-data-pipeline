"use client";

import {
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import type { Channel } from "@/lib/data";
import { formatCompact, formatPct } from "@/lib/format";

interface Point {
  x: number;
  y: number;
  name: string;
}

function ScatterTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: { payload: Point }[];
}) {
  if (!active || !payload?.length) return null;
  const p = payload[0].payload;
  return (
    <div className="tooltip">
      <div className="tooltip-name">{p.name}</div>
      <div className="tooltip-value">{formatCompact(p.x)} inscritos</div>
      <div className="tooltip-value">{formatPct(p.y)} engajamento</div>
    </div>
  );
}

export default function PositioningScatter({
  channels,
  selectedId,
}: {
  channels: Channel[];
  selectedId: string;
}) {
  const build = (list: Channel[]): Point[] =>
    list
      .filter((c) => c.subscriber_count && c.engagement_rate != null)
      .map((c) => ({
        x: c.subscriber_count as number,
        y: c.engagement_rate as number,
        name: c.title,
      }));

  const others = build(channels.filter((c) => c.channel_id !== selectedId));
  const selected = build(channels.filter((c) => c.channel_id === selectedId));

  return (
    <ResponsiveContainer width="100%" height={320}>
      <ScatterChart margin={{ top: 8, right: 20, bottom: 28, left: 4 }}>
        <CartesianGrid stroke="var(--grid)" strokeDasharray="3 3" />
        <XAxis
          type="number"
          dataKey="x"
          name="Inscritos"
          scale="log"
          domain={["auto", "auto"]}
          tickFormatter={(v) => formatCompact(v)}
          tick={{ fill: "var(--muted)", fontSize: 11 }}
          tickLine={false}
          axisLine={{ stroke: "var(--baseline)" }}
          label={{
            value: "Inscritos (escala log)",
            position: "bottom",
            fill: "var(--muted)",
            fontSize: 12,
          }}
        />
        <YAxis
          type="number"
          dataKey="y"
          name="Engajamento"
          tickFormatter={(v) => formatPct(v)}
          tick={{ fill: "var(--muted)", fontSize: 11 }}
          tickLine={false}
          axisLine={{ stroke: "var(--baseline)" }}
        />
        <ZAxis range={[80, 80]} />
        <Tooltip content={<ScatterTooltip />} cursor={{ strokeDasharray: "3 3" }} />
        <Legend
          wrapperStyle={{ fontSize: 12, color: "var(--text-secondary)" }}
        />
        <Scatter name="Outros canais" data={others} fill="var(--series-muted)" />
        <Scatter name="Seu canal" data={selected} fill="var(--series-1)" />
      </ScatterChart>
    </ResponsiveContainer>
  );
}
