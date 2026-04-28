# Image commune pour producer + consumers + dashboard + report
FROM python:3.10-slim

WORKDIR /app

# Outils systeme
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Dependances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code applicatif
COPY . .

# Par defaut, on lance le producer (override via docker-compose)
CMD ["python", "producer.py"]
