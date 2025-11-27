import pandas as pd
import gcsfs
import joblib
import os
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
import numpy as np

# --- CONFIGURAÇÕES ---
BUCKET_NAME = "datalake-imoveis-pdm-2025"
GOLD_FILE_PATH = f"gs://{BUCKET_NAME}/gold/imoveis_venda_analise.parquet"
MODEL_LOCAL_PATH = "model_imoveis_xgb.pkl"
MODEL_CLOUD_PATH = f"gs://{BUCKET_NAME}/models/model_imoveis_xgb.pkl"

def train():
    print("⏳ [1/6] Iniciando download explícito do arquivo Gold...")
    
    # 1. Download Seguro (Evita timeout)
    fs = gcsfs.GCSFileSystem()
    local_gold_file = "gold_temp.parquet"
    
    try:
        if not os.path.exists(local_gold_file):
            print("   ⬇️ Baixando para o disco local...")
            fs.get(GOLD_FILE_PATH, local_gold_file)
        else:
            print("   ℹ️ Arquivo local já existe, usando cache.")
            
    except Exception as e:
        print(f"   ❌ Erro no download: {e}")
        return

    print("📖 [2/6] Lendo e preparando dados...")
    df = pd.read_parquet(local_gold_file)
    
    # --- AJUSTE DE COLUNAS (Baseado no seu debug) ---
    target = 'target_price'
    
    # Vamos usar a Área e o Tipo do imóvel
    # O modelo vai aprender: "Apartamento de 100m² custa X"
    
    # Passo A: Limpar nulos na área e no preço
    df_clean = df.dropna(subset=[target, 'total_area_m2'])
    
    # Passo B: Converter 'property_type' (texto) para número (código)
    # Ex: UNIT -> 1, HOME -> 2
    if 'property_type' in df_clean.columns:
        df_clean['property_type_code'] = df_clean['property_type'].astype('category').cat.codes
        feature_col_type = 'property_type_code'
    else:
        feature_col_type = None

    # Definição final das features
    features = ['total_area_m2']
    if feature_col_type:
        features.append(feature_col_type)
        
    print(f"   ✅ Features usadas: {features}")
    
    X = df_clean[features]
    y = df_clean[target]

    print(f"   ✅ Total de imóveis válidos para treino: {len(X)}")

    # Separação Treino (80%) vs Teste (20%)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("🧠 [3/6] Iniciando treinamento com XGBoost...")
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        random_state=42,
        n_jobs=-1
    )
    
    model.fit(X_train, y_train)
    print("   ✅ Modelo treinado!")

    print("📊 [4/6] Avaliando performance...")
    predictions = model.predict(X_test)
    
    r2 = r2_score(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)

    print("-" * 40)
    print("🏆 RESULTADOS FINAIS:")
    print(f"   ⭐ Acurácia (R²): {r2:.4f}")
    print(f"   💰 Erro Médio (MAE): R$ {mae:,.2f}")
    print("-" * 40)

    print("💾 [5/6] Salvando modelo localmente...")
    joblib.dump(model, MODEL_LOCAL_PATH)

    print("☁️ [6/6] Enviando cérebro da IA para o Bucket...")
    try:
        fs.put(MODEL_LOCAL_PATH, MODEL_CLOUD_PATH)
        print(f"   🚀 Sucesso! Modelo salvo em: {MODEL_CLOUD_PATH}")
    except Exception as e:
        print(f"   ❌ Erro ao subir modelo: {e}")
    
    # Limpeza
    if os.path.exists(MODEL_LOCAL_PATH):
        os.remove(MODEL_LOCAL_PATH)

if __name__ == "__main__":
    train()