"use client";

import { useMemo, useState } from "react";
import type { Channel } from "@/lib/data";

interface Props {
  channels: Channel[];
  selectedId: string;
  onSelect: (id: string) => void;
}

export default function ChannelPicker({ channels, selectedId, onSelect }: Props) {
  const [query, setQuery] = useState("");

  const filtered = useMemo(() => {
    const term = query.trim().toLowerCase();
    if (!term) return channels;
    return channels.filter(
      (c) =>
        c.handle.toLowerCase().includes(term) ||
        c.title.toLowerCase().includes(term),
    );
  }, [channels, query]);

  return (
    <div className="picker">
      <p className="card-label">Seu canal / comparação</p>
      <input
        className="search"
        type="text"
        placeholder="Insira o handle do seu canal (ex.: @FelipeNeto)"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
      />
      <div className="chips">
        {filtered.map((c) => (
          <button
            key={c.channel_id}
            className={`chip ${c.channel_id === selectedId ? "active" : ""}`}
            onClick={() => onSelect(c.channel_id)}
            type="button"
          >
            {c.handle}
            {c.is_owner && <span className="owner-tag">seu canal</span>}
          </button>
        ))}
        {filtered.length === 0 && (
          <span style={{ color: "var(--muted)", fontSize: 13 }}>
            Nenhum canal encontrado — este canal ainda não está na watchlist.
          </span>
        )}
      </div>
    </div>
  );
}
