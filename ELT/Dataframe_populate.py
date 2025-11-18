# -*- coding: utf-8 -*-
import logging
import time
import random
import unicodedata
import os
import datetime
import base64
import json

import pandas as pd
from typing import List, Optional, Dict, Any
import cloudscraper

from google.cloud import storage
from flask import Flask, request

# ===================== Constantes =====================

# --- Constantes do Scraper ---
ORIGIN_PAGE = "https://www.zapimoveis.com.br/"
API_URL = "https://glue-api.zapimoveis.com.br/v4/listings"
DEVICE_ID = "c5a40c3c-d033-4a5d-b1e2-79b59e4fb68d"
PORTAL = "ZAP"
CATEGORY_PAGE = "RESULT"
LISTING_TYPE = "USED"

# --- Constantes de Comportamento ---
SIZE = 30
FROM_MAX = 300
PRICE_MIN_START = 1000
PRICE_STEP = 49990  # Este passo pode ser grande para RENTAL, considere enviar via parâmetro
REQUESTS_TIMEOUT = 30
BASE_SLEEP_SECONDS = 0.9
RANDOM_JITTER_MAX = 0.6
RETRIES = 5
USE_BROWSER_COOKIES = False  # Correto para Cloud Run

# --- Configurações de Ambiente e GCS ---
GCS_BUCKET_NAME = "pdm-especulacoes-imobiliario"
# O CSV foi removido, pois agora recebemos parâmetros

INCLUDE_FIELDS = (
    "expansion(search(result(listings(listing("
    "expansionType,contractType,listingsCount,propertyDevelopers,sourceId,displayAddressType,amenities,usableAreas,"
    "constructionStatus,listingType,description,title,stamps,createdAt,floors,unitTypes,nonActivationReason,providerId,"
    "propertyType,unitSubTypes,unitsOnTheFloor,legacyId,id,portal,unitFloor,parkingSpaces,updatedAt,address,suites,"
    "publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,advertiserContact,whatsappNumber,bedrooms,"
    "acceptExchange,pricingInfos,showPrice,resale,buildings,capacityLimit,status,priceSuggestion,condominiumName,modality,"
    "enhancedDevelopment),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,legacyZapId,createdDate,tier,"
    "trustScore,totalCountByFilter,totalCountByAdvertiser),medias,accountLink,link,children(id,usableAreas,totalAreas,"
    "bedrooms,bathrooms,parkingSpaces,pricingInfos))),totalCount)),fullUriFragments,nearby(search(result(listings(listing("
    "expansionType,contractType,listingsCount,propertyDevelopers,sourceId,displayAddressType,amenities,usableAreas,"
    "constructionStatus,listingType,description,title,stamps,createdAt,floors,unitTypes,nonActivationReason,providerId,"
    "propertyType,unitSubTypes,unitsOnTheFloor,legacyId,id,portal,unitFloor,parkingSpaces,updatedAt,address,suites,"
    "publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,advertiserContact,whatsappNumber,bedrooms,"
    "acceptExchange,pricingInfos,showPrice,resale,buildings,capacityLimit,status,priceSuggestion,condominiumName,modality,"
    "enhancedDevelopment),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,legacyZapId,createdDate,tier,"
    "trustScore,totalCountByFilter,totalCountByAdvertiser),medias,accountLink,link,children(id,usableAreas,totalAreas,"
    "bedrooms,bathrooms,parkingSpaces,pricingInfos))),totalCount)),page,search(result(listings(listing("
    "expansionType,contractType,listingsCount,propertyDevelopers,sourceId,displayAddressType,amenities,usableAreas,"
    "constructionStatus,listingType,description,title,stamps,createdAt,floors,unitTypes,nonActivationReason,providerId,"
    "propertyType,unitSubTypes,unitsOnTheFloor,legacyId,id,portal,unitFloor,parkingSpaces,updatedAt,address,suites,"
    "publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,advertiserContact,whatsappNumber,bedrooms,"
    "acceptExchange,pricingInfos,showPrice,resale,buildings,capacityLimit,status,priceSuggestion,condominiumName,modality,"
    "enhancedDevelopment),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,legacyZapId,createdDate,tier,"
    "trustScore,totalCountByFilter,totalCountByAdvertiser),medias,accountLink,link,children(id,usableAreas,totalAreas,"
    "bedrooms,bathrooms,parkingSpaces,pricingInfos))),totalCount),topoFixo(search(result(listings(listing("
    "expansionType,contractType,listingsCount,propertyDevelopers,sourceId,displayAddressType,amenities,usableAreas,"
    "constructionStatus,listingType,description,title,stamps,createdAt,floors,unitTypes,nonActivationReason,providerId,"
    "propertyType,unitSubTypes,unitsOnTheFloor,legacyId,id,portal,unitFloor,parkingSpaces,updatedAt,address,suites,"
    "publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,advertiserContact,whatsappNumber,bedrooms,"
    "acceptExchange,pricingInfos,showPrice,resale,buildings,capacityLimit,status,priceSuggestion,condominiumName,modality,"
    "enhancedDevelopment),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,legacyZapId,createdDate,tier,"
    "trustScore,totalCountByFilter,totalCountByAdvertiser),medias,accountLink,link,children(id,usableAreas,totalAreas,"
    "bedrooms,bathrooms,parkingSpaces,pricingInfos))),totalCount))"
)

# ===================== Logging =====================

logger = logging.getLogger("zap_pipeline_v4")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)


# ===================== Helpers =====================

def _ascii_no_accents(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")


def build_address_location_id(state: str, city: str) -> str:
    st = _ascii_no_accents(state)
    ct = _ascii_no_accents(city)
    return f"BR>{st}>NULL>{ct}"


UA_EDGE_141 = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
)

COMMON_HEADERS = {
    "User-Agent": UA_EDGE_141,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8,en-US;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Origin": "https://www.zapimoveis.com.br",
    "Referer": "https://www.zapimoveis.com.br/",
    "x-deviceid": DEVICE_ID,
    "x-domain": ".zapimoveis.com.br",
}


def make_scraper():
    s = cloudscraper.create_scraper()
    s.headers.update(COMMON_HEADERS)
    return s


def bootstrap_cookies() -> Dict[str, str]:
    # (Lógica original mantida)
    s = make_scraper()
    try:
        r = s.get(ORIGIN_PAGE, timeout=REQUESTS_TIMEOUT)
        logger.info(f"Bootstrap origem: {r.status_code}")
    except Exception as e:
        logger.warning(f"Falha ao abrir origem: {e}")
    cookies = s.cookies.get_dict()
    keys = ", ".join(cookies.keys()) if cookies else "(nenhum)"
    logger.info(f"Cookies coletados: {keys}")
    if "cf_clearance" not in cookies and "__cf_bm" not in cookies:
        logger.warning(
            "cf_clearance/__cf_bm NÃO encontrados — se houver 403/HTML, importe do navegador (USE_BROWSER_COOKIES=True).")
    return cookies


def bootstrap_from_browser() -> Dict[str, str]:
    # (Lógica original mantida)
    try:
        import browser_cookie3
    except Exception:
        logger.error("Instale browser-cookie3 para importar cookies.")
        raise
    merged = {}
    try:
        merged.update(browser_cookie3.chrome(domain_name="zapimoveis.com.br").get_dict())
    except Exception as e:
        logger.warning(f"Erro lendo cookies zapimoveis.com.br: {e}")
    try:
        merged.update(browser_cookie3.chrome(domain_name="glue-api.zapimoveis.com.br").get_dict())
    except Exception as e:
        logger.warning(f"Erro lendo cookies glue-api.zapimoveis.com.br: {e}")
    logger.info("Cookies (browser) importados: " + (", ".join(merged.keys()) if merged else "(nenhum)"))
    return merged


def polite_sleep():
    time.sleep(BASE_SLEEP_SECONDS + random.uniform(0, RANDOM_JITTER_MAX))


def looks_like_html(text: str) -> bool:
    if not text:
        return False
    t = text.lstrip()
    return t.startswith("<") or t.lower().startswith("<!doctype")


def extract_listings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # tenta caminhos conhecidos
    exp = (((payload or {}).get("expansion") or {})
                          .get("search") or {}
                          .get("result") or {})
    if isinstance(exp, dict) and "listings" in exp:
        return exp.get("listings") or []
    srch = ((payload or {}).get("search") or {}).get("result") or {}
    return srch.get("listings") or []


# ===================== Core: chamada da API =====================

def call_api(scraper, params: Dict[str, str], tries=RETRIES):
    # (Lógica original mantida)
    last = None
    for i in range(1, tries + 1):
        try:
            r = scraper.get(API_URL, params=params, timeout=REQUESTS_TIMEOUT)
            ct = (r.headers.get("Content-Type") or "").lower()
            if r.status_code == 200 and "application/json" in ct:
                if looks_like_html(r.text):
                    raise ValueError("Corpo HTML com status 200")
                return r
            if r.status_code == 429 or 500 <= r.status_code < 600:
                wait = 1.2 * (2 ** (i - 1)) + random.uniform(0, 0.8)
                logger.warning(f"{r.status_code} na API (tentativa {i}/{tries}). Backoff {wait:.1f}s…")
                time.sleep(wait)
                last = r
                continue
            if r.status_code in (401, 403) or ("text/html" in ct) or looks_like_html(r.text):
                logger.warning(f"Bloqueio/HTML (status={r.status_code}, ct={ct}). Tentativa {i}/{tries}.")
                time.sleep(0.8 + random.uniform(0, 0.6))
                last = r
                continue
            last = r
        except Exception as e:
            logger.warning(f"Exceção na chamada (tentativa {i}/{tries}): {e}")
            time.sleep(1.0 + random.uniform(0, 0.6))
    return last


# ===================== Upload para GCS =====================

def upload_df_to_gcs(
        df: pd.DataFrame,
        bucket_name: str,
        destination_blob_name: str,
        format: str = 'parquet'
):
    """
    Converte um DataFrame para o formato especificado (parquet ou csv)
    e faz o upload para o Google Cloud Storage.
    """
    # Cloud Run/Functions têm um sistema de arquivos /tmp gravável na memória
    temp_filename = os.path.join("/tmp", destination_blob_name.split('/')[-1])

    try:
        if format == 'parquet':
            df.to_parquet(temp_filename, index=False, engine='pyarrow')
        elif format == 'csv':
            df.to_csv(temp_filename, index=False, encoding='utf-8')
        else:
            raise ValueError("Formato de arquivo não suportado. Use 'parquet' ou 'csv'.")

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(temp_filename)
        logger.info(f"Arquivo salvo no GCS: gs://{bucket_name}/{destination_blob_name}")

    except Exception as e:
        logger.error(f"Falha ao fazer upload para o GCS: {e}")
        raise
    finally:
        # Limpa o arquivo temporário
        if os.path.exists(temp_filename):
            os.remove(temp_filename)


# ===================== Pipeline =====================

def run_pipeline(
        city: str,
        state: str,
        business_type: str,
        price_max: int  # <-- [CORREÇÃO DE LÓGICA] Recebe o preço máximo
) -> (Optional[pd.DataFrame], Dict[str, Any]):
    """
    Executa o pipeline de scraping para UMA cidade e UM tipo de negócio.
    Retorna um DataFrame com os dados e um dicionário com metadados.
    """

    start_time = time.time()
    start_time_utc = datetime.datetime.utcnow()
    logger.info(f"Iniciando pipeline para {city}/{state} (Business: {business_type}, PriceMax: {price_max})")

    # 1) Cookies
    cookies = bootstrap_from_browser() if USE_BROWSER_COOKIES else bootstrap_cookies()

    # 2) Scraper da API
    scraper_api = make_scraper()
    if cookies:
        scraper_api.cookies.update(cookies)

    # 3) Base estática de params
    address_loc_id = build_address_location_id(state, city)

    base_params = {
        "user": DEVICE_ID,
        "portal": PORTAL,
        "categoryPage": CATEGORY_PAGE,
        "business": business_type,
        "listingType": LISTING_TYPE,
        "__zt": "mtc:deduplication2023",
        "addressCity": city,
        "addressLocationId": address_loc_id,
        "addressState": state,
        "addressType": "city",
        "size": str(SIZE),
        "images": "webp",
        "includeFields": INCLUDE_FIELDS,
    }

    rows: List[Dict[str, Any]] = []
    status = "SUCCESS"

    # 4) Varredura por FAIXAS
    try:
        # [CORREÇÃO DE LÓGICA] Usa o 'price_max' recebido como parâmetro
        for pmin in range(PRICE_MIN_START, price_max, PRICE_STEP):
            pmax = min(pmin + PRICE_STEP, price_max)
            logger.info(f"🔎 Faixa R$ {pmin} .. R$ {pmax}")

            for from_v in range(0, FROM_MAX, SIZE):
                page = (from_v // SIZE) + 1
                params = dict(base_params)
                params.update({
                    "page": str(page),
                    "from": str(from_v),
                    "priceMin": str(pmin),
                    "priceMax": str(pmax),
                })

                r = call_api(scraper_api, params)
                if r is None:
                    logger.error(f"❌ Sem resposta na faixa {pmin}-{pmax} from={from_v}")
                    break

                # ... (restante da lógica de paginação e extração) ...
                ct = (r.headers.get("Content-Type") or "").lower()
                if r.status_code == 404:
                    logger.info("⚠️ 404 (fim dos dados nesta faixa).")
                    break

                if r.status_code != 200 or "application/json" not in ct:
                    snippet = (r.text or "")[:400].replace("\n", " ")
                    logger.error(f"❌ Resposta inesperada: status={r.status_code} ct={ct} corpo[400]={snippet}")
                    break

                try:
                    data = r.json()
                except Exception as e:
                    snippet = (r.text or "")[:200].replace("\n", " ")
                    logger.error(f"❌ JSON inválido (from={from_v}): {e} | corpo[200]={snippet}")
                    time.sleep(random.uniform(1.0, 2.2))
                    continue

                listings = extract_listings(data)
                if not listings:
                    logger.info("ℹ️ Nenhum listing retornado; encerrando paginação desta faixa.")
                    break

                for it in listings:
                    lin = it.get("listing") or {}
                    lin["account"] = it.get("account")
                    lin["medias"] = it.get("medias")
                    lin["accountLink"] = it.get("accountLink")
                    lin["link"] = it.get("link")
                    rows.append(lin)

                logger.info(f"✔️ page={page} from={from_v} registros={len(listings)}")
                polite_sleep()

                if len(listings) < SIZE:
                    logger.info("ℹ️ Página final detectada (menos que SIZE).")
                    break

            time.sleep(random.uniform(1.2, 2.5))

    except Exception as e:
        logger.error(f"Erro fatal durante o scraping de {city}/{business_type}: {e}")
        status = f"FAILURE: {str(e)}"

    # Geração de DF e Metadados
    df = None
    total_records = 0
    size_in_bytes = 0

    if not rows:
        logger.warning(f"⚠️ Nenhum dado coletado para {city}/{state} ({business_type}).")
        if status == "SUCCESS":
            status = "NO_DATA"
    else:
        try:
            df = pd.json_normalize(rows, sep=".")
            total_records = len(df)
            size_in_bytes = int(df.memory_usage(deep=True).sum())
            logger.info(f"✅ Coleta finalizada | shape={df.shape} | bytes={size_in_bytes}")
        except Exception as e:
            logger.error(f"Erro ao normalizar dados: {e}")
            status = f"FAILURE_NORMALIZE: {str(e)}"

    end_time = time.time()
    end_time_utc = datetime.datetime.utcnow()
    total_duration = end_time - start_time

    # Cria o dicionário de metadados
    metadata = {
        "execution_start_utc": start_time_utc.isoformat(),
        "execution_end_utc": end_time_utc.isoformat(),
        "total_duration_seconds": total_duration,
        "status": status,
        "city": city,
        "state": state,
        "business_type": business_type,
        "total_records": total_records,
        "data_size_bytes": size_in_bytes,
        "parameters": {
            "price_min": PRICE_MIN_START,
            "price_max": price_max,  # [CORREÇÃO DE LÓGICA] Salva o preço correto
            "price_step": PRICE_STEP,
            "page_size": SIZE,
            "offset_limit": FROM_MAX,
            "listing_type": LISTING_TYPE,
            "device_id": DEVICE_ID
        }
    }

    return df, metadata


# ===================== [NOVO PONTO DE ENTRADA] =====================

# [CORREÇÃO DE ARQUITETURA]
# Substituímos o 'main()' que lia CSV por um servidor Flask.
# Este servidor escuta por requisições (do Pub/Sub) e processa UMA tarefa.

app = Flask(__name__)


@app.route("/", methods=["POST"])
def handle_task():
    """
    Ponto de entrada para requisições HTTP (ex: vindas do Pub/Sub).
    Espera uma mensagem JSON no corpo da requisição.
    """

    # 1. Valida e decodifica a mensagem (padrão Pub/Sub Push)
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        logger.error("Requisição inválida: formato de envelope Pub/Sub esperado.")
        return "Requisição inválida", 400

    message = envelope["message"]
    data = message.get("data")

    if not data:
        logger.error("Requisição inválida: 'data' não encontrado na mensagem.")
        return "Requisição inválida", 400

    # 2. Decodifica o payload (parâmetros da tarefa)
    try:
        payload_str = base64.b64decode(data).decode("utf-8")
        payload = json.loads(payload_str)

        city = payload.get("city")
        state = payload.get("state")
        business_type = payload.get("business_type")
        price_max = int(payload.get("price_max"))  # Essencial

        if not all([city, state, business_type, price_max]):
            raise ValueError("Parâmetros 'city', 'state', 'business_type', 'price_max' são obrigatórios.")

    except Exception as e:
        logger.error(f"Erro ao decodificar payload: {e}")
        return f"Payload inválido: {e}", 400

    # 3. Executa o pipeline com os parâmetros recebidos
    try:
        logger.info(f"--- Iniciando processamento para: {city}, {state} ({business_type}) ---")
        (data_df, exec_metadata) = run_pipeline(
            city=city,
            state=state,
            business_type=business_type,
            price_max=price_max
        )

        run_timestamp = exec_metadata['execution_start_utc'].replace(":", "-")
        base_path = f"{business_type}/{state}/{city}/{run_timestamp}"

        # 4. Salva Metadados
        try:
            metadata_df = pd.DataFrame([exec_metadata])
            meta_blob_name = f"metadata/{base_path}_metadata.parquet"
            upload_df_to_gcs(metadata_df, GCS_BUCKET_NAME, meta_blob_name, format='parquet')
        except Exception as e:
            logger.error(f"Falha ao salvar metadados para {city}/{business_type}: {e}")
            # Retorna 500 para o Pub/Sub tentar novamente (retry)
            return f"Erro ao salvar metadados: {e}", 500

        if data_df is not None and not data_df.empty:
            try:
                data_blob_name = f"data/{base_path}_data.parquet"
                upload_df_to_gcs(data_df, GCS_BUCKET_NAME, data_blob_name, format='parquet')
            except Exception as e:
                logger.error(f"Falha ao salvar dados para {city}/{business_type}: {e}")
                # Retorna 500 para o Pub/Sub tentar novamente (retry)
                return f"Erro ao salvar dados: {e}", 500
        else:
            logger.warning(f"Nenhum dado de listing encontrado para {city}/{business_type}. Tabela de dados não será criada.")

        logger.info(f"--- Finalizado: {business_type} para {city}, {state} ---")
        return "OK", 200

    except Exception as e:
        logger.error(f"Erro fatal no pipeline para {city}/{business_type}: {e}")
        return f"Erro no pipeline: {e}", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)