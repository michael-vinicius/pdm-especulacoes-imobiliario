# Dockerfile
FROM python:3.11-slim

# evitar prompts
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# copiar requirements primeiro (cache)
COPY requirements.txt .

RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# copiar todo o projeto
COPY . .

# porta que o Cloud Run passa via env PORT
ENV PORT=8080

# Ajuste o worker / threads conforme necessidade
# IMPORTANTE: indique o path do módulo + app (veja nota abaixo)
CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:${PORT} --workers 1 --threads 8 ELT.Dataframe_populate:app"]
