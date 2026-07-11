# Watchlist Analytics — Front-end (Next.js)

Dashboard minimalista para o cliente comparar o próprio canal com os concorrentes
monitorados, usando os dados da **camada gold** do datalake.

## Como rodar

Pré-requisito: Node.js 18.18+ (não estava instalado no ambiente onde o projeto
foi gerado — instale localmente).

```bash
cd frontend
npm install
npm run dev
```

Abra http://localhost:3000.

## Fluxo dos dados

```
datalake/gold/*.parquet
        │
        │  python data_collect/05_export_gold_to_json.py   (rode após a pipeline)
        ▼
frontend/data/gold.json   ← consumido pela UI (import estático)
```

Sempre que rodar a pipeline (`04_pipeline_watchlist_monitor.py`), regenere o JSON
com o script `05_export_gold_to_json.py` para atualizar o dashboard.

## Insights gerados (a partir da gold)

| Componente | Origem (gold) | O que responde |
|---|---|---|
| KPIs do canal | `mart_channel_snapshot` + `mart_channel_growth` | inscritos, engajamento, cadência, posição e Δ vs. snapshot anterior |
| Ranking por inscritos | `mart_channel_snapshot` | quem lidera; seu canal em destaque |
| Taxa de engajamento | `mart_channel_snapshot` | qualidade da audiência vs. tamanho |
| Cadência de postagem | `mart_channel_snapshot` | ritmo de produção dos concorrentes |
| Posicionamento (scatter) | `mart_channel_snapshot` | tamanho × engajamento |
| Evolução de inscritos | `fact_channel_stats` (série temporal) | crescimento entre snapshots |
| Movimentação no ranking | `mart_channel_growth` | subiu / desceu / entrou / saiu do top 5 |

## Design

- Layout minimalista, tema claro/escuro automático (`prefers-color-scheme`).
- Paleta validada (dataviz skill): destaque em azul (`--series-1`) para o canal
  selecionado, neutro para os demais — combinação segura para daltonismo.
- Gráficos com Recharts; badges de status sempre com ícone + rótulo (nunca só cor).

## Estrutura

```
frontend/
├── app/
│   ├── layout.tsx        # shell + metadata
│   ├── page.tsx          # dashboard (orquestra os componentes)
│   └── globals.css       # paleta + estilos minimalistas
├── components/
│   ├── ChannelPicker.tsx     # busca/seleção do "seu canal"
│   ├── KpiTiles.tsx          # 4 KPIs do canal selecionado
│   ├── BarComparison.tsx     # barras (ranking, engajamento, cadência)
│   ├── PositioningScatter.tsx
│   ├── GrowthChart.tsx
│   └── MovementTable.tsx
├── lib/
│   ├── data.ts           # tipos do bundle gold
│   └── format.ts         # formatação de números/datas
└── data/
    └── gold.json         # exportado da camada gold
```

> Observação: o input do handle hoje seleciona entre os canais **já presentes na
> watchlist** (dados fixos da gold). Coletar um canal novo em tempo real exige
> acionar a pipeline Python — próximo passo natural (expor a pipeline via API).
