FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DATABASE_PATH=/data/gateway.db \
    KEYS_FILE=/config/keys.json

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 非 root 运行；/data 存 SQLite（挂卷持久化），/config 挂密钥环
RUN useradd -r -u 10001 gateway && mkdir -p /data /config && chown gateway /data
USER gateway
VOLUME ["/data"]

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/healthz')"

CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
