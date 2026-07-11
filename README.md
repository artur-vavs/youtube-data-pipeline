# Channel Analytics

Pipeline de dados do YouTube com arquitetura medallion (bronze → silver → gold) e
front-end para análise histórica e comparativa de canais.

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
- **Frequência de ingestão:** 1x ao dia.
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
