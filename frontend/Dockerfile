FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py data_access.py ./
COPY .streamlit ./.streamlit

ENV GOLD_DIR=/app/datalake/gold \
    PYTHONUNBUFFERED=1
EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
