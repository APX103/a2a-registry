FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

RUN mkdir -p /app/data

ENV REGISTRY_DB_URL=sqlite:////app/data/registry.db
ENV REGISTRY_PROBE_INTERVAL=60
ENV REGISTRY_HOST=0.0.0.0
ENV REGISTRY_PORT=8006

EXPOSE 8006
CMD ["python", "server.py"]
