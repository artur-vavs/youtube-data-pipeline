# Channel Analytics — frontend Streamlit

Dashboard analítico que lê diretamente os arquivos Parquet da camada **gold** e
da camada operacional **observability**.
Não há etapa de exportação para JSON nem dependências Node/TypeScript.

## Executar com Docker

Na raiz do projeto:

```bash
docker compose up --build
```

Acesse <http://localhost:8501>. O diretório `datalake/gold` é montado como
somente leitura no container. Depois de uma nova execução da pipeline, use o
botão **Recarregar dados** na barra lateral.

## Executar localmente

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r frontend/requirements.txt
streamlit run frontend/app.py
```

Por padrão, o app busca a gold em `datalake/gold`. Para usar outro local:

```bash
GOLD_DIR=/caminho/para/gold streamlit run frontend/app.py
```

## Conteúdo analítico

| Área | Tabelas gold | Visuais |
|---|---|---|
| Visão geral | `mart_channel_snapshot` | KPIs, ranking, engajamento, cadência e dispersão tamanho × engajamento |
| Evolução | `fact_channel_stats` | séries de inscritos, visualizações e posição |
| Vídeos | `fact_video_stats` + `dim_video` | ranking dos vídeos e tabela detalhada |
| Ranking | `mart_channel_growth` | posição, deltas, movimentação e transições do top 5 |
| Observabilidade | `observability/pipeline_runs` + `api_calls` | status, duração, última atualização, volume processado e erros da API |

Os filtros suportam múltiplos clientes/watchlists e até dez canais por comparação.

## Camada de observabilidade

Cada execução da pipeline registra:

- `pipeline_runs.parquet`: início/fim, duração, status (`success`, `partial` ou
  `error`), canais solicitados/processados, quantidade de chamadas, erros e
  linhas produzidas;
- `api_calls.parquet`: endpoint, recurso, duração em milissegundos, status HTTP,
  tipo e mensagem do erro.

Os arquivos ficam em `datalake/observability` e são montados como somente leitura
no frontend. A aba **Observabilidade** mostra a execução mais recente e o
histórico das últimas 20 execuções.

Para testar a atualização automática a cada 5 minutos, execute na raiz:

```bash
docker compose --profile test up --build
```
