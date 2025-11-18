import logging
from ELT.Dataframe_populate import run_pipeline

# Configura o logging para ver o que está acontecendo
logger = logging.getLogger("zap_pipeline_v4")
logger.setLevel(logging.INFO)
h = logging.StreamHandler()
h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(h)

if __name__ == "__main__":
    print("--- INICIANDO TESTE LOCAL DO PIPELINE ---")

    # Parâmetros de teste (use uma cidade pequena para ser rápido)
    TEST_CITY = "Goiania"
    TEST_STATE = "Goias"
    TEST_BUSINESS = "RENTAL"
    TEST_PRICE_MAX = 100000  # Teste com o limite de 100k

    try:
        (df, metadata) = run_pipeline(
            city=TEST_CITY,
            state=TEST_STATE,
            business_type=TEST_BUSINESS,
            price_max=TEST_PRICE_MAX
        )

        print("\n--- TESTE CONCLUÍDO ---")
        print("Metadados Gerados:")
        print(metadata)

        if df is not None:
            print(f"\nDataFrame criado com shape: {df.shape}")
        else:
            print("\nNenhum dado retornado (verificar status nos metadados).")

    except Exception as e:
        print(f"\n--- ERRO NO TESTE LOCAL ---")
        print(f"Erro: {e}")