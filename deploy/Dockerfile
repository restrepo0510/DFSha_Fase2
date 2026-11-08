FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables openssl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dfsha ./dfsha
COPY scripts ./scripts
COPY bench ./bench
COPY tests ./tests
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
# Una sola imagen; el rol se elige con el comando: python -m dfsha.mds.server | python -m dfsha.dn.server | python -m dfsha.client
