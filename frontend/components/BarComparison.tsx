"use client";

import {
  Bar,
  BarChart,
  Cell,
  LabelList,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export interface BarRow {
  id: string;
  name: string;
  value: number;
}

interface Props {
  rows: BarRow[];
  selectedId: string;
  format: (value: number) => string;
}

function ChartTooltip({
  active,
  payload,
  format,
}: {
  active?: boolean;
  payload?: { payload: BarRow }[];
  format: (value: number) => string;
}) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return (
    <div className="tooltip">
      <div className="tooltip-name">{row.name}</div>
      <div className="tooltip-value">{format(row.value)}</div>
    </div>
  );
}

export default function BarComparison({ rows, selectedId, format }: Props) {
  const sorted = [...rows].sort((a, b) => b.value - a.value);
  const height = sorted.length * 38 + 16;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart
        data={sorted}
        layout="vertical"
        margin={{ top: 4, right: 64, bottom: 4, left: 8 }}
      >
        <XAxis type="number" hide domain={[0, "dataMax"]} />
        <YAxis
          type="category"
          dataKey="name"
          width={132}
          tick={{ fill: "var(--muted)", fontSize: 12 }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          cursor={{ fill: "var(--grid)", opacity: 0.35 }}
          content={<ChartTooltip format={format} />}
        />
        <Bar dataKey="value" radius={[0, 4, 4, 0]} isAnimationActive={false}>
          {sorted.map((row) => (
            <Cell
              key={row.id}
              fill={
                row.id === selectedId ? "var(--series-1)" : "var(--series-muted)"
              }
            />
          ))}
          <LabelList
            dataKey="value"
            position="right"
            formatter={(value) => format(Number(value))}
            style={{ fill: "var(--text-secondary)", fontSize: 12 }}
          />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
