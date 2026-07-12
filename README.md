# Channel Analytics

Pipeline de dados do YouTube com arquitetura medallion (bronze → silver → gold) e
dashboard Streamlit para análise histórica e comparativa de canais.

## Premissa

- Coletar dados de canais pré-definidos de categorias como **esporte**,
  **entretenimento** e **games**, através do *handle* do canal.
- Armazenar uma carga inicial por canal na camada **bronze** — cada canal tem o
  seu próprio arquivo parquet.
- Na camada **silver**, aplicar **idempotência** para acompanhar o crescimento do
  canal a cada chamada (6 chamadas por dia, a cada 4 horas). Capturamos métricas
  como `view_count`, `subscriber_count`, `like_count`, `comment_count`, entre
  outras — tanto dos vídeos quanto do próprio canal.
- Na camada **gold**, montar a camada analítica desses dados.

---

## 3.1 Identidade

- **Nome do projeto:** Channel Analytics
- **Tema:** Tema 11 — União de todos os temas (Top 5 de cada tema)
- **Data de início:** 09/07/2026

### Equipe

- Hemersson do Vale
- Alan Pacheco
- Artur Vinicius

---

## 3.2 Problema e Propósito

- **Problema:** falta de acesso, pelo público, a dados analíticos e históricos
  dos canais mais bem rankeados do YouTube.
- **Propósito:** fornecer uma análise histórica e temporal dos maiores canais —
  total de views, contagem de inscritos e vídeos publicados.
- **Público-alvo:** consumidores de grandes canais do YouTube e pessoas que
  pretendem abrir um canal e querem se basear nos dados dos seus canais favoritos
  ou de referência.
- **Hipótese de valor:** ao seguir os passos de um canal modelo, é possível obter
  um impulso mais rápido de views e inscritos, viabilizando uma monetização mais
  ágil.

---

## 3.3 Escopo Técnico

### Fonte de dados

- Canais do YouTube, vídeos dos canais e playlists.
- **Frequência de ingestão:** configurável; o modo de produção pode ser diário e
  o modo de testes abaixo executa a cada 5 minutos.
- **Métrica principal:** análise temporal de dados-chave.
- **Perguntas analíticas:** crescimento diário, projeção de monetização e
  frequência de inscritos.

### Fora do escopo

- Análises estatísticas aprofundadas.
- Front end muito aprimorado

---

## 3.4 Critério de Sucesso

### Definição de pronto

- Visualização dos dados em um front-end.

### Riscos

- API sem cota disponível.

---

## Dashboard Streamlit

O frontend lê a camada `datalake/gold` diretamente e permite:

- selecionar uma watchlist e destacar o canal do cliente;
- comparar inscritos, views, engajamento e cadência de publicação;
- acompanhar a evolução temporal e a movimentação no ranking;
- analisar os vídeos com mais visualizações e seus indicadores de interação.

Para executar:

```bash
docker compose up --build
```

Abra <http://localhost:8501>. Consulte `frontend/README.md` para a execução local
e para o mapa completo entre visuais e tabelas da camada gold.

As execuções também alimentam `datalake/observability`, com histórico de status,
duração, atualização dos dados, chamadas à API e erros detalhados. Essas
informações estão disponíveis na aba **Observabilidade** do dashboard.
O mesmo histórico é sincronizado automaticamente em
`datalake/observability/observability.db`, nas tabelas `pipeline_runs`,
`api_calls` e `layer_metrics`. Para reconstruir o banco a partir dos Parquets,
execute `python data_collect/06_build_observability_db.py`.

### Execução de teste a cada 5 minutos

Para subir o dashboard e um worker que executa a pipeline a cada 5 minutos:

```bash
docker compose --profile test up --build
```

O worker usa `PIPELINE_INTERVAL_MINUTES=5` apenas no serviço de teste. Para uma
execução única, mantenha o comando usual:

```bash
python data_collect/04_pipeline_watchlist_monitor.py
```

O intervalo também pode ser alterado sem editar o código:

```bash
PIPELINE_INTERVAL_MINUTES=10 python data_collect/04_pipeline_watchlist_monitor.py
```
