# job_main.py
import os
import json
import pandas as pd
import logging
from ELT.Dataframe_populate import run_pipeline, upload_df_to_gcs

# Logger
logger = logging.getLogger("zap_job")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

def main():
    # Parâmetros do job via ENV vars
    city = os.environ.get("CITY")
    state = os.environ.get("STATE")
    business_type = os.environ.get("BUSINESS_TYPE")
    price_max = int(os.environ.get("PRICE_MAX", "5000000"))

    if not all([city, state, business_type]):
        logger.error("Variáveis CITY, STATE e BUSINESS_TYPE são obrigatórias")
        exit(1)

    df, metadata = run_pipeline(
        city=city,
        state=state,
        business_type=business_type,
        price_max=price_max
    )

    run_timestamp = metadata['execution_start_utc'].replace(":", "-")
    base_path = f"{business_type}/{state}/{city}/{run_timestamp}"

    # Upload de metadados
    meta_blob_name = f"metadata/{base_path}_metadata.parquet"
    upload_df_to_gcs(pd.DataFrame([metadata]), os.environ.get("GCS_BUCKET_NAME"), meta_blob_name)

    # Upload de dados, se houver
    if df is not None and not df.empty:
        data_blob_name = f"data/{base_path}_data.parquet"
        upload_df_to_gcs(df, os.environ.get("GCS_BUCKET_NAME"), data_blob_name)

    logger.info("Job finalizado com sucesso.")

if __name__ == "__main__":
    main()
