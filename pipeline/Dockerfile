FROM python:3.13-slim

WORKDIR /app

COPY pipeline/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY data_collect ./data_collect
COPY config ./config

ENV PYTHONUNBUFFERED=1

CMD ["python", "data_collect/04_pipeline_watchlist_monitor.py", "--interval-minutes", "5"]
