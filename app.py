import os
import joblib
import pandas as pd
import gcsfs
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

# --- CONFIGURAÇÕES ---
# Nome do Bucket e caminhos
BUCKET_NAME = "datalake-imoveis-pdm-2025"  # <--- CONFIRA SE ESTÁ CERTO
MODEL_CLOUD_PATH = f"gs://{BUCKET_NAME}/models/model_imoveis_xgb.pkl"
MODEL_LOCAL_PATH = "model_imoveis_xgb.pkl"

# Variável global para guardar o modelo na memória
model = None

# --- ESTRUTURA DOS DADOS DE ENTRADA ---
class ImovelInput(BaseModel):
    total_area_m2: float
    property_type_slug: str = "APARTMENT" # Ex: 'APARTMENT', 'HOME', 'UNIT'

# --- CICLO DE VIDA (LIGAR/DESLIGAR) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Isso roda quando a API liga
    global model
    print("⏳ [API] Inicializando... Baixando modelo do Data Lake...")
    
    try:
        # Baixa o modelo do Google Cloud Storage
        fs = gcsfs.GCSFileSystem()
        if fs.exists(MODEL_CLOUD_PATH):
            fs.get(MODEL_CLOUD_PATH, MODEL_LOCAL_PATH)
            print("   ⬇️ Download concluído.")
            
            # Carrega o modelo na memória RAM
            model = joblib.load(MODEL_LOCAL_PATH)
            print("   🧠 Cérebro da IA carregado com sucesso!")
            
            # Remove arquivo temporário para limpar disco
            if os.path.exists(MODEL_LOCAL_PATH):
                os.remove(MODEL_LOCAL_PATH)
        else:
            print(f"   ❌ Erro: Modelo não encontrado em {MODEL_CLOUD_PATH}")
    except Exception as e:
        print(f"   ❌ Falha crítica ao carregar modelo: {e}")

    yield
    # Isso roda quando a API desliga (limpeza)
    print("🛑 [API] Desligando...")

# --- INICIALIZA O APP ---
app = FastAPI(title="API Previsão Imóveis", lifespan=lifespan)

# --- ROTAS ---

@app.get("/")
def home():
    return {"status": "online", "message": "API de Imóveis rodando! Acesse /docs para testar."}

@app.post("/predict")
def predict(imovel: ImovelInput):
    if not model:
        raise HTTPException(status_code=500, detail="Modelo de IA não está carregado.")

    # 1. Tratamento dos dados (Feature Engineering em Tempo Real)
    # Precisamos converter o texto 'APARTMENT' para número, igual fizemos no treino
    # Como não temos o dicionário exato, vamos fazer uma aproximação simples para funcionar
    tipo_code = 0 # Default
    tipo = imovel.property_type_slug.upper()
    
    if "HOME" in tipo or "CASA" in tipo:
        tipo_code = 1
    elif "UNIT" in tipo or "CONJUNTO" in tipo:
        tipo_code = 2
    # Apartment/Outros fica como 0

    # 2. Monta o DataFrame igual ao usado no treino
    # A ordem das colunas DEVE ser a mesma do treino: ['total_area_m2', 'property_type_code']
    input_data = pd.DataFrame([[imovel.total_area_m2, tipo_code]], 
                              columns=['total_area_m2', 'property_type_code'])

    # 3. Faz a previsão
    try:
        preco_estimado = model.predict(input_data)[0]
        return {
            "area_m2": imovel.total_area_m2,
            "tipo": tipo,
            "preco_previsto": float(round(preco_estimado, 2)),
            "mensagem": f"O valor estimado para este imóvel é R$ {preco_estimado:,.2f}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na previsão: {str(e)}")

# Se rodar este arquivo direto, inicia o servidor local
if __name__ == "__main__":
    import uvicorn
    # Roda na porta 8080 (padrão do Google Cloud Run)
    uvicorn.run(app, host="0.0.0.0", port=8080)