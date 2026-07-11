"use client";

import type { Channel } from "@/lib/data";
import { formatCompact, formatSigned } from "@/lib/format";

const MOVEMENT: Record<string, { icon: string; cls: string; label: string }> = {
  subiu: { icon: "▲", cls: "badge-good", label: "subiu" },
  desceu: { icon: "▼", cls: "badge-critical", label: "desceu" },
  estavel: { icon: "■", cls: "badge-neutral", label: "estável" },
  novo: { icon: "＋", cls: "badge-neutral", label: "novo" },
};

const TOP5: Record<string, { icon: string; cls: string; label: string }> = {
  entrou: { icon: "▲", cls: "badge-good", label: "entrou no top 5" },
  saiu: { icon: "▼", cls: "badge-critical", label: "saiu do top 5" },
  permaneceu: { icon: "●", cls: "badge-neutral", label: "no top 5" },
  fora: { icon: "○", cls: "badge-neutral", label: "fora do top 5" },
  novo: { icon: "＋", cls: "badge-neutral", label: "novo" },
};

function Badge({ map, key_ }: { map: typeof MOVEMENT; key_: string | null }) {
  const info = (key_ && map[key_]) || { icon: "–", cls: "badge-neutral", label: "—" };
  return (
    <span className={`badge ${info.cls}`}>
      <span aria-hidden>{info.icon}</span>
      {info.label}
    </span>
  );
}

export default function MovementTable({
  channels,
  selectedId,
}: {
  channels: Channel[];
  selectedId: string;
}) {
  const rows = [...channels].sort((a, b) => (a.rank ?? 99) - (b.rank ?? 99));

  return (
    <table className="mvt-table">
      <thead>
        <tr>
          <th>#</th>
          <th>Canal</th>
          <th>Inscritos</th>
          <th>Δ inscritos</th>
          <th>Movimento</th>
          <th>Top 5</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((c) => (
          <tr
            key={c.channel_id}
            className={c.channel_id === selectedId ? "mvt-row-selected" : ""}
          >
            <td>{c.rank ?? "—"}</td>
            <td className="mvt-name">{c.title}</td>
            <td>{formatCompact(c.subscriber_count)}</td>
            <td>{c.delta_subscribers == null ? "—" : formatSigned(c.delta_subscribers)}</td>
            <td>
              <Badge map={MOVEMENT} key_={c.movement} />
            </td>
            <td>
              <Badge map={TOP5} key_={c.status_top5} />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
