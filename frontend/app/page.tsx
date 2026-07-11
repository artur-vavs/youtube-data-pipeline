"use client";

import { useMemo, useState } from "react";
import goldRaw from "@/data/gold.json";
import type { Channel, Gold } from "@/lib/data";
import { formatCompact, formatDate, formatPct } from "@/lib/format";
import ChannelPicker from "@/components/ChannelPicker";
import KpiTiles from "@/components/KpiTiles";
import BarComparison, { type BarRow } from "@/components/BarComparison";
import PositioningScatter from "@/components/PositioningScatter";
import GrowthChart from "@/components/GrowthChart";
import MovementTable from "@/components/MovementTable";

const gold = goldRaw as unknown as Gold;

export default function Home() {
  const channels = gold.channels;
  const defaultId =
    channels.find((c) => c.is_owner)?.channel_id ?? channels[0]?.channel_id ?? "";

  const [selectedId, setSelectedId] = useState<string>(defaultId);

  const selected = useMemo<Channel | undefined>(
    () => channels.find((c) => c.channel_id === selectedId),
    [channels, selectedId],
  );

  if (!selected) {
    return <main className="container">Sem dados na camada gold.</main>;
  }

  const subsRows: BarRow[] = channels
    .filter((c) => c.subscriber_count != null)
    .map((c) => ({ id: c.channel_id, name: c.title, value: c.subscriber_count as number }));

  const engRows: BarRow[] = channels
    .filter((c) => c.engagement_rate != null)
    .map((c) => ({ id: c.channel_id, name: c.title, value: c.engagement_rate as number }));

  const cadRows: BarRow[] = channels
    .filter((c) => c.videos_per_week != null)
    .map((c) => ({ id: c.channel_id, name: c.title, value: c.videos_per_week as number }));

  const series = gold.series.by_channel[selectedId] ?? [];

  return (
    <main className="container">
      <header className="header">
        <p className="eyebrow">Watchlist Analytics</p>
        <h1 className="title">Compare o seu canal com a concorrência</h1>
        <p className="subtitle">
          Cliente <strong>{gold.clients[0]?.client_name}</strong> · {channels.length}{" "}
          canais monitorados · snapshot de {formatDate(gold.generated_at)}
        </p>
      </header>

      <ChannelPicker
        channels={channels}
        selectedId={selectedId}
        onSelect={setSelectedId}
      />

      <KpiTiles channel={selected} total={channels.length} />

      <div className="grid grid-2 section-gap">
        <div className="card">
          <p className="card-label">Ranking por inscritos</p>
          <p className="card-sub">Seu canal em destaque. Top 5 = os 5 primeiros.</p>
          <BarComparison rows={subsRows} selectedId={selectedId} format={formatCompact} />
        </div>

        <div className="card">
          <p className="card-label">Taxa de engajamento</p>
          <p className="card-sub">(likes + comentários) ÷ views, média ponderada.</p>
          <BarComparison rows={engRows} selectedId={selectedId} format={formatPct} />
        </div>
      </div>

      <div className="grid grid-2 section-gap">
        <div className="card">
          <p className="card-label">Cadência de postagem</p>
          <p className="card-sub">Vídeos por semana no período coletado.</p>
          <BarComparison
            rows={cadRows}
            selectedId={selectedId}
            format={(v) => `${v.toFixed(1)}/sem`}
          />
        </div>

        <div className="card">
          <p className="card-label">Posicionamento: tamanho × engajamento</p>
          <p className="card-sub">
            Canais grandes tendem a engajar menos — onde o seu se posiciona?
          </p>
          <PositioningScatter channels={channels} selectedId={selectedId} />
        </div>
      </div>

      <div className="grid grid-2 section-gap">
        <div className="card">
          <p className="card-label">Evolução de inscritos — {selected.title}</p>
          <p className="card-sub">Crescimento entre os snapshots diários acumulados.</p>
          <GrowthChart series={series} />
        </div>

        <div className="card">
          <p className="card-label">Movimentação no ranking</p>
          <p className="card-sub">Variação de posição e entrada/saída do top 5.</p>
          <div style={{ overflowX: "auto" }}>
            <MovementTable channels={channels} selectedId={selectedId} />
          </div>
        </div>
      </div>

      <p className="footnote">
        Dados da camada <strong>gold</strong> do datalake (marts
        <code> mart_channel_snapshot</code> e <code>mart_channel_growth</code>).
        Atualize com <code>python data_collect/05_export_gold_to_json.py</code> após
        cada execução da pipeline.
      </p>
    </main>
  );
}
