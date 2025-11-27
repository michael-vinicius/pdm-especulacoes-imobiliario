#!/bin/bash

# Define onde está a chave (dentro do container, ela estará na raiz /app)
export GOOGLE_APPLICATION_CREDENTIALS="trabalho-pdm-imoveis-3775c96e52ca.json"

# Inicia o Uvicorn
# O $PORT é injetado automaticamente pelo Google Cloud Run (geralmente 8080)
uvicorn app:app --host 0.0.0.0 --port $PORT