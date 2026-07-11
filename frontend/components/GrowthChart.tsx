"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { SeriesPoint } from "@/lib/data";
import { formatCompact, formatDate, formatInt } from "@/lib/format";

interface Row {
  label: string;
  subscribers: number | null;
}

function GrowthTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: { payload: Row }[];
}) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return (
    <div className="tooltip">
      <div className="tooltip-name">{row.label}</div>
      <div className="tooltip-value">{formatInt(row.subscribers)} inscritos</div>
    </div>
  );
}

export default function GrowthChart({ series }: { series: SeriesPoint[] }) {
  if (series.length < 2) {
    return (
      <p style={{ color: "var(--muted)", fontSize: 13, margin: "24px 0" }}>
        A evolução aparece a partir do 2º snapshot diário acumulado. Há apenas{" "}
        {series.length} snapshot(s) no histórico.
      </p>
    );
  }

  const rows: Row[] = series.map((p) => ({
    label: formatDate(p.ingested_at),
    subscribers: p.subscriber_count,
  }));

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={rows} margin={{ top: 8, right: 24, bottom: 4, left: 8 }}>
        <CartesianGrid stroke="var(--grid)" vertical={false} />
        <XAxis
          dataKey="label"
          tick={{ fill: "var(--muted)", fontSize: 11 }}
          tickLine={false}
          axisLine={{ stroke: "var(--baseline)" }}
        />
        <YAxis
          domain={["auto", "auto"]}
          tickFormatter={(v) => formatCompact(v)}
          tick={{ fill: "var(--muted)", fontSize: 11 }}
          tickLine={false}
          axisLine={false}
          width={52}
        />
        <Tooltip content={<GrowthTooltip />} />
        <Line
          type="monotone"
          dataKey="subscribers"
          stroke="var(--series-1)"
          strokeWidth={2}
          dot={{ r: 4, fill: "var(--series-1)" }}
          activeDot={{ r: 6 }}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
