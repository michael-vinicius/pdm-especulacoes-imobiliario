# -*- coding: utf-8 -*-
"""
Zap Imóveis v4 scraper (faixas de preço, contrato v4 "params" como no navegador)
Requisitos:
    pip install cloudscraper pandas
Opcional:
    pip install browser-cookie3
"""

import time
import logging
import random
import unicodedata
from typing import List, Optional, Dict, Any

import pandas as pd

# ===================== Constantes =====================

ORIGIN_PAGE = "https://www.zapimoveis.com.br/"
API_URL     = "https://glue-api.zapimoveis.com.br/v4/listings"

# Identidade / filtros padrão (ajuste conforme seu caso)
DEVICE_ID       = "c5a40c3c-d033-4a5d-b1e2-79b59e4fb68d"
PORTAL          = "ZAP"
CATEGORY_PAGE   = "RESULT"
BUSINESS        = "SALE"   # "RENTAL" para aluguel
LISTING_TYPE    = "USED"

# Local (exemplo; você pode passar lat/lon ou deixar None)
ADDRESS_CITY    = "Goiânia"
ADDRESS_STATE   = "Goiás"       # exibido; AddressLocationId será sem acentos
ADDRESS_LAT     = None          # "-23.555771"
ADDRESS_LON     = None          # "-46.639557"
ADDRESS_TYPE    = "city"

# Paginação e varredura por faixa de preço
SIZE            = 30
FROM_MAX        = 300           # varre from = 0, 30, 60... < FROM_MAX
PRICE_MIN_START = 1000
PRICE_STEP      = 49990
PRICE_MAX_END   = 10000000

# Arquivo
CSV_PATH        = "backup/Goiania.csv"

# Comportamento de rede
REQUESTS_TIMEOUT   = 30
BASE_SLEEP_SECONDS = 0.9
RANDOM_JITTER_MAX  = 0.6
RETRIES            = 5
USE_BROWSER_COOKIES = False  # mude para True se precisar importar cookies do navegador

# includeFields (contrato v4). Pode colar exatamente o mesmo que o navegador gerou.
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
    # no exemplo do navegador, ambos vêm sem acento e com espaços preservados
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
    "Accept-Encoding": "gzip, deflate",  # <-- REMOVIDO o 'br'
    "Origin": "https://www.zapimoveis.com.br",
    "Referer": "https://www.zapimoveis.com.br/",
    "x-deviceid": DEVICE_ID,
    "x-domain": ".zapimoveis.com.br",
}

def make_scraper():
    import cloudscraper
    s = cloudscraper.create_scraper()
    s.headers.update(COMMON_HEADERS)
    return s

def bootstrap_cookies() -> Dict[str, str]:
    # Abre a origem para o cloudscraper resolver CF e pegar cookies
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
        logger.warning("cf_clearance/__cf_bm NÃO encontrados — se houver 403/HTML, importe do navegador (USE_BROWSER_COOKIES=True).")
    return cookies

def bootstrap_from_browser() -> Dict[str, str]:
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

# ===================== Core: chamada da API v4 (params) =====================

def call_api(scraper, params: Dict[str, str], tries=RETRIES):
    last = None
    for i in range(1, tries + 1):
        try:
            r = scraper.get(API_URL, params=params, timeout=REQUESTS_TIMEOUT)
            ct = (r.headers.get("Content-Type") or "").lower()
            if r.status_code == 200 and "application/json" in ct:
                # proteção extra contra corpo HTML
                if looks_like_html(r.text):
                    raise ValueError("Corpo HTML com status 200")
                return r
            # 429/5xx -> backoff
            if r.status_code == 429 or 500 <= r.status_code < 600:
                wait = 1.2 * (2 ** (i - 1)) + random.uniform(0, 0.8)
                logger.warning(f"{r.status_code} na API (tentativa {i}/{tries}). Backoff {wait:.1f}s…")
                time.sleep(wait)
                last = r
                continue
            # 403/HTML: tenta rebootstrap leve
            if r.status_code in (401, 403) or ("text/html" in ct) or looks_like_html(r.text):
                logger.warning(f"Bloqueio/HTML (status={r.status_code}, ct={ct}). Tentativa {i}/{tries}.")
                time.sleep(0.8 + random.uniform(0, 0.6))
                last = r
                continue
            # demias códigos não-OK
            last = r
        except Exception as e:
            logger.warning(f"Exceção na chamada (tentativa {i}/{tries}): {e}")
            time.sleep(1.0 + random.uniform(0, 0.6))
    return last

# ===================== Pipeline por faixas =====================

def run_pipeline() -> Optional[pd.DataFrame]:
    # 1) Cookies
    cookies = bootstrap_from_browser() if USE_BROWSER_COOKIES else bootstrap_cookies()

    # 2) Scraper da API com cookies injetados
    scraper_api = make_scraper()
    if cookies:
        scraper_api.cookies.update(cookies)

    # 3) Base estática de params (idêntica ao navegador)
    address_loc_id = build_address_location_id(ADDRESS_STATE, ADDRESS_CITY)

    base_params = {
        "user": DEVICE_ID,
        "portal": PORTAL,
        "categoryPage": CATEGORY_PAGE,
        "business": BUSINESS,
        "listingType": LISTING_TYPE,
        "__zt": "mtc:deduplication2023",
        "addressCity": ADDRESS_CITY,
        "addressLocationId": address_loc_id,
        "addressState": ADDRESS_STATE,
        "addressType": ADDRESS_TYPE,
        "size": str(SIZE),
        "images": "webp",
        "includeFields": INCLUDE_FIELDS,  # pode remover se preferir usar o padrão do servidor
    }
    if ADDRESS_LAT and ADDRESS_LON:
        base_params["addressPointLat"] = str(ADDRESS_LAT)
        base_params["addressPointLon"] = str(ADDRESS_LON)

    rows: List[Dict[str, Any]] = []

    # 4) Varredura por FAIXAS (priceMin/priceMax)
    for pmin in range(PRICE_MIN_START, PRICE_MAX_END, PRICE_STEP):
        pmax = min(pmin + PRICE_STEP, PRICE_MAX_END)
        logger.info(f"🔎 Faixa R$ {pmin} .. R$ {pmax}")

        # 4a) paginação por offset (from) até esgotar ou bater limite
        for from_v in range(0, FROM_MAX, SIZE):
            page = (from_v // SIZE) + 1
            params = dict(base_params)  # shallow copy
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

            ct = (r.headers.get("Content-Type") or "").lower()
            if r.status_code == 404:
                logger.info("⚠️ 404 (fim dos dados nesta faixa).")
                break

            if r.status_code != 200 or "application/json" not in ct:
                snippet = (r.text or "")[:400].replace("\n", " ")
                logger.error(f"❌ Resposta inesperada: status={r.status_code} ct={ct} corpo[400]={snippet}")
                # em bloqueio/HTML, pare a faixa para não martelar
                break

            try:
                data = r.json()
            except Exception as e:
                snippet = (r.text or "")[:200].replace("\n", " ")
                logger.error(f"❌ JSON inválido (from={from_v}): {e} | corpo[200]={snippet}")
                # tenta próxima página/offset com pequena pausa
                time.sleep(random.uniform(1.0, 2.2))
                continue

            listings = extract_listings(data)
            if not listings:
                logger.info("ℹ️ Nenhum listing retornado; encerrando paginação desta faixa.")
                break

            # Achata cada item numa linha (preservando a estrutura principal)
            for it in listings:
                lin = it.get("listing") or {}
                lin["account"] = it.get("account")
                lin["medias"] = it.get("medias")
                lin["accountLink"] = it.get("accountLink")
                lin["link"] = it.get("link")
                rows.append(lin)

            logger.info(f"✔️ page={page} from={from_v} registros={len(listings)}")
            polite_sleep()

            # heurística de última página (lista menor que SIZE)
            if len(listings) < SIZE:
                logger.info("ℹ️ Página final detectada (menos que SIZE).")
                break

        # pausa entre faixas
        time.sleep(random.uniform(1.2, 2.5))

    if not rows:
        logger.warning("⚠️ Nenhum dado coletado.")
        return None

    # Normalização leve (json_normalize já lida com nested dicts)
    df = pd.json_normalize(rows, sep=".")
    df.to_csv(CSV_PATH, index=False, encoding="utf-8")
    logger.info(f"✅ Salvo em {CSV_PATH} | shape={df.shape}")
    return df

# ===================== Execução =====================

if __name__ == "__main__":
    df = run_pipeline()
    if df is not None:
        print(df.head(3))
