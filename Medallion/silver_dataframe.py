import argparse
import glob
import json
import os
import re
from typing import List, Optional, Any

import numpy as np
import pandas as pd
from dateutil import parser as dtparser

# ---------- helpers ----------

AMENITY_MAP = {
    # normalizações simples PT/EN para chave estável em snake_case
    "AIR_CONDITIONING": "air_conditioning",
    "AR-CONDICIONADO": "air_conditioning",
    "BALCONY": "balcony",
    "VARANDA": "balcony",
    "BARBECUE_GRILL": "barbecue_grill",
    "CHURRASQUEIRA": "barbecue_grill",
    "SERVICE_AREA": "service_area",
    "ÁREA DE SERVIÇO": "service_area",
    "HOME_OFFICE": "home_office",
    "CLOSET": "closet",
    "GARAGE": "garage",
    "GARAGEM": "garage",
    "POOL": "pool",
    "PISCINA": "pool",
    "DINNER_ROOM": "dining_room",
    "SALA DE JANTAR": "dining_room",
    "GYM": "gym",
    "ACADEMIA": "gym",
    "ELEVATOR": "elevator",
    "ELECTRONIC_GATE": "electronic_gate",
    "DISABLED_ACCESS": "disabled_access",
    "PETS_ALLOWED": "pets_allowed",
    "GATED_COMMUNITY": "gated_community",
    "CONCIERGE_24H": "concierge_24h",
    "BICYCLES_PLACE": "bicycles_place",
    "SPORTS_COURT": "sports_court",
    "GARDEN": "garden",
    "BUILTIN_WARDROBE": "builtin_wardrobe",
    "BLINDEX_BOX": "blindex_box",
    "KITCHEN_CABINETS": "kitchen_cabinets",
}


def _to_decimal(x: Any) -> Optional[float]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip().replace(".", "").replace(",", ".")  # suporta "1.234,56"
    try:
        return float(s)
    except Exception:
        return None


def _first_non_null(*vals):
    for v in vals:
        if v is not None and not (isinstance(v, float) and np.isnan(v)):
            return v
    return None


def _norm_str(x: Any) -> Optional[str]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    return s if s != "" else None


def _snake(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or None


def _to_ts(x: Any) -> Optional[pd.Timestamp]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    try:
        return pd.to_datetime(dtparser.parse(str(x)))
    except Exception:
        return None


def _explode_array(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    CORRIGIDO: Garante que a coluna array seja tratada como lista antes de explodir.
    Se a coluna não existir, retorna um DataFrame vazio com as colunas do df original.
    """
    if col not in df.columns:
        # Se a coluna não existe, retorna o ID e uma coluna vazia
        return df.drop(columns=[c for c in df.columns if c != "id"]).assign(**{col: [[]] * len(df)}).explode(col,
                                                                                                             ignore_index=True)

    def ensure_list(v):
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, (np.ndarray, pd.Series)):
            return v.tolist()
        # Tratamento de string JSON para lista de dicts (comum em dados brutos)
        if isinstance(v, str):
            txt = v.strip()
            # Tenta carregar JSON se parecer um array
            if txt.startswith("[") and txt.endswith("]"):
                try:
                    # Tenta substituir ' por " (compatibilidade JSON)
                    return json.loads(txt.replace("'", '"'))
                except Exception:
                    # Se falhar, trata como um array de um único item (a string)
                    return [txt]
            # Se for uma string simples, trata como array de um item
            return [txt]
        if isinstance(v, float) and np.isnan(v):
            return []
        return [v]

    df = df.copy()
    # Aplica a função para garantir que todos os valores na coluna 'col' sejam listas
    df[col] = df[col].apply(ensure_list)

    # Explode apenas o ID e a coluna array
    df_explode = df[["id", col]].explode(col, ignore_index=True)

    # Filtra linhas onde o valor da coluna explode é nulo/vazio (e não era para ser explodido)
    df_explode = df_explode[~df_explode[col].isna()].reset_index(drop=True)

    return df_explode


# ---------- núcleo Silver ----------

def build_silver_tables(bronze_paths: List[str], outdir: str):
    os.makedirs(outdir, exist_ok=True)

    # 1) leitura do bronze (um ou muitos arquivos)
    paths = []
    for p in bronze_paths:
        if p.startswith("gs://"):
            # Se for nuvem, adiciona o caminho direto (glob não funciona no GCS sem plugin extra)
            paths.append(p)
        else:
            # Se for local, usa o glob para achar arquivos
            paths.extend(glob.glob(p))
    if not paths:
        raise FileNotFoundError("Nenhum arquivo Bronze encontrado pelos padrões fornecidos.")
    dfb = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)

    # 2) dedup *antes* de transformar
    dedup_col = "id" if "id" in dfb.columns else "title"
    if dedup_col in dfb.columns:
        sort_cols = [c for c in ["updatedAt_ts", "createdAt_ts"] if c in dfb.columns]
        dfb = dfb.sort_values(by=[dedup_col] + sort_cols, ascending=[True] + [False] * len(sort_cols),
                              kind="mergesort")
        dfb = dfb.drop_duplicates(subset=[dedup_col], keep="first")

    # Garante que 'id' exista para usar como 'listing_id'
    if 'id' not in dfb.columns:
        print("⚠️ Aviso: A coluna 'id' não existe no Bronze, cannot gerar listing_id e tabelas explode.")
        return

        # -------------------------
    # A) silver_listings (1:1)
    # -------------------------
    listings_cols_keep = {
        # ... (restante das colunas de listings_cols_keep omitidas por brevidade, mas o mapeamento é mantido)
        "id": "listing_id",
        "sourceId": "source_id",
        "providerId": "provider_id",
        "portal": "portal",
        "account_id": "account_id",
        "status": "status",
        "statusEncoded": "status_encoded",
        "listingType": "listing_type",
        "publicationType": "publication_type",
        "modality": "modality",
        "contractType": "contract_type",
        "propertyType": "property_type",
        "usableAreas_num": "usable_area_m2",
        "totalAreas": "total_area_raw",
        "bedrooms_num": "bedrooms",
        "suites_num": "suites",
        "bathrooms_num": "bathrooms",
        "parkingSpaces_num": "parking_spaces",
        "unitFloor_num": "unit_floor",
        "unitsOnTheFloor_num": "units_on_floor",
        "buildings_num": "buildings",
        "floors_num": "floors",
        "createdAt_ts": "created_at",
        "updatedAt_ts": "updated_at",
        "deliveredAt_ts": "delivered_at",
        "address_city": "address_city_raw",
        "address_state": "address_state_raw",
        "address_stateAcronym": "state_acronym",
        "address_zone": "address_zone_raw",
        "address_district": "address_district_raw",
        "address_neighborhood": "address_neighborhood_raw",
        "address_street": "address_street_raw",
        "address_streetNumber": "address_number_raw",
        "address_zipCode": "zip_code",
        "address_point_lat_num": "lat",
        "address_point_lon_num": "lon",
        "title_clean": "title",
        "description_clean": "description",
        "qualityScores_lqsV3_num": "quality_lqs_v3",
        "qualityScores_lqsBeta_num": "quality_lqs_beta",
        "showPrice": "show_price",
        "acceptExchange": "accept_exchange",
        "transacted": "transacted",
    }

    # garante existência/renomeia
    present = {k: v for k, v in listings_cols_keep.items() if k in dfb.columns}
    dfl = dfb[list(present.keys())].rename(columns=present).copy()

    # normalizações leves
    for c in ["status", "listing_type", "publication_type", "modality", "contract_type", "property_type"]:
        if c in dfl.columns:
            dfl[c] = dfl[c].apply(_norm_str).apply(_snake)

    # normaliza total_area se existir (para float)
    if "total_area_raw" in dfl.columns:
        dfl["total_area_m2"] = dfl["total_area_raw"].apply(_to_decimal)

    # coordenadas coerentes
    if "lat" in dfl.columns:
        dfl["lat"] = dfl["lat"].astype("float64", errors='ignore')
    if "lon" in dfl.columns:
        dfl["lon"] = dfl["lon"].astype("float64", errors='ignore')

    # derive um bounding flag simples (tem geolocalização?)
    if "lat" in dfl.columns and "lon" in dfl.columns:
        dfl["has_geo"] = (~dfl["lat"].isna()) & (~dfl["lon"].isna())
    else:
        dfl["has_geo"] = False

    # -------------------------
    # B) silver_pricing (explode)
    # -------------------------

    # 💡 CORREÇÃO DE EXPLODE E NORMALIZAÇÃO
    dfp = pd.DataFrame(columns=["listing_id"])

    # Ajuste para minúsculo para garantir que encontra a coluna do Bronze
    # Bloco corrigido
    if "pricinginfos_arr" in dfb.columns:
        # Usa 'id' (porque vem do Bronze) e 'pricinginfos_arr' (minúsculo corrigido)
        dfp_temp = _explode_array(dfb[["id", "pricinginfos_arr"]], "pricinginfos_arr")

        if not dfp_temp.empty and any(isinstance(x, dict) for x in dfp_temp["pricinginfos_arr"].dropna()):
            pi = pd.json_normalize(dfp_temp["pricinginfos_arr"]).rename(columns={
                "iptuPeriod": "iptu_period",
                "businessType": "business_type",
                "monthlyCondoFee": "monthly_condo_fee",
                "yearlyIptu": "yearly_iptu",
                "price": "price",
                "iptu": "iptu",
                "rentalInfo.period": "rental_period",
                "rentalInfo.warranties": "rental_warranties",
                "rentalInfo.monthlyRentalTotalPrice": "monthly_rental_total_price"
            })
            # Aqui sim removemos a coluna array e juntamos
            dfp = pd.concat([dfp_temp.drop(columns=["pricinginfos_arr"]), pi], axis=1)
            # AQUI acontece a mágica: o 'id' vira 'listing_id'
            dfp = dfp.rename(columns={"id": "listing_id"}).copy()


            # tipagem monetária (float)
            for col in ["price", "iptu", "monthly_condo_fee", "yearly_iptu", "monthly_rental_total_price"]:
                if col in dfp.columns:
                    dfp[col + "_raw"] = dfp[col].astype(str).replace('<NA>', None)
                    dfp[col] = dfp[col].apply(_to_decimal)

            # normaliza businessType/period
            for col in ["business_type", "rental_period", "iptu_period"]:
                if col in dfp.columns:
                    dfp[col] = dfp[col].apply(_norm_str).apply(_snake)

            # métrica derivada: aluguel_total_mensal
            def _aluguel_total(row):
                if row.get("business_type") != "rental":
                    return None
                m_total = row.get("monthly_rental_total_price")
                if m_total is not None and not np.isnan(m_total):
                    return m_total

                base = _first_non_null(row.get("price"))
                condo = _first_non_null(row.get("monthly_condo_fee")) or 0.0
                iptu_m = None

                if row.get("iptu_period") == "monthly":
                    iptu_m = row.get("iptu")
                elif row.get("iptu_period") in (None, "", "yearly") and row.get("iptu") is not None:
                    iptu_m = row.get("iptu") / 12.0

                return (base or 0.0) + condo + (iptu_m or 0.0)

            dfp["monthly_total_rent"] = dfp.apply(_aluguel_total, axis=1)
            dfp = dfp[["listing_id"] + [c for c in dfp.columns if c != "listing_id"]].copy()

        else:
            # Garante que o dfp vazio tenha as colunas mínimas esperadas se o explode falhar ou for vazio
            expected_cols = ["listing_id", "business_type", "price", "price_raw", "monthly_total_rent"]
            dfp = pd.DataFrame(columns=expected_cols)

    # -------------------------
    # C) silver_medias (explode)
    # -------------------------
    # 💡 CORREÇÃO DE EXPLODE E NORMALIZAÇÃO
    dfm = pd.DataFrame(columns=["listing_id"])

    if "medias_arr" in dfb.columns:
        dfm_temp = _explode_array(dfb[["id", "medias_arr"]], "medias_arr")

        if not dfm_temp.empty and any(isinstance(x, dict) for x in dfm_temp["medias_arr"].dropna()):
            mi = pd.json_normalize(dfm_temp["medias_arr"]).rename(
                columns={"id": "media_id", "url": "media_url", "type": "media_type"})
            dfm = pd.concat([dfm_temp.drop(columns=["medias_arr"]), mi], axis=1)
            dfm = dfm.rename(columns={"id": "listing_id"})

            if "media_type" in dfm.columns:
                dfm["media_type"] = dfm["media_type"].apply(_norm_str).apply(_snake)
            dfm = dfm[["listing_id"] + [c for c in dfm.columns if c != "listing_id"]].copy()

        else:
            expected_cols = ["listing_id", "media_id", "media_url", "media_type"]
            dfm = pd.DataFrame(columns=expected_cols)

    # -------------------------
    # D) silver_amenities (explode + normaliza)
    # -------------------------
    # 💡 CORREÇÃO DE EXPLODE
    dfa = pd.DataFrame(columns=["listing_id", "amenity"])

    amen_col = None
    for c in ["mergedAmenities_arr", "amenities_arr", "aiAmenities_arr", "searchableAmenities_arr"]:
        if c in dfb.columns:
            amen_col = c
            break

    if amen_col:
        # Explode para criar uma linha por amenity
        dfa_temp = _explode_array(dfb[["id", amen_col]], amen_col)

        if not dfa_temp.empty:
            dfa = dfa_temp.rename(columns={amen_col: "amenity_raw", "id": "listing_id"})

            # normaliza para chave estável
            def map_amen(a):
                if a is None or (isinstance(a, float) and np.isnan(a)):
                    return None
                s = str(a).strip() if a is not None else None
                if not s:
                    return None
                # Tenta mapear por chave de catálogo
                key = AMENITY_MAP.get(s.upper())
                if key:
                    return key
                # fallback: snake genérico
                return _snake(s)

            dfa["amenity"] = dfa["amenity_raw"].apply(map_amen)
            # A filtragem por dropna deve ocorrer *depois* da verificação de empty
            dfa = dfa.dropna(subset=["amenity"]).drop_duplicates(["listing_id", "amenity"])

        else:
            # Garante que o dfa vazio tenha as colunas mínimas esperadas
            dfa = pd.DataFrame(columns=["listing_id", "amenity_raw", "amenity"])

    # -------------------------
    # Escrita
    # -------------------------

    # Garante que o listing_id seja str antes de salvar, para consistência.
    dfl["listing_id"] = dfl["listing_id"].astype(str)

    # Prepara as tabelas explode para escrita:
    for df in [dfp, dfm, dfa]:
        if "listing_id" in df.columns:
            df["listing_id"] = df["listing_id"].astype(str)
        else:
            # Se a tabela explode está vazia e sem listing_id, garante a coluna
            df["listing_id"] = pd.Series([], dtype=str)

    # Partição: O método `to_parquet` do Pandas não oferece partição automática como em Spark.
    # Para ter partição, precisaríamos escrever em um loop (ex: por 'portal' ou 'state_acronym').
    # No entanto, a forma mais simples (sem loops e sem re-leitura) é gerar o arquivo único.
    # O código abaixo **não particiona** no sentido de *hive-style partitioning* (pastas),
    # mas garante que os dados estão logicamente separados em tabelas (o objetivo Silver).

    dfl.to_parquet(os.path.join(outdir, "silver_listings.parquet"), engine="pyarrow", index=False)

    # Garante as colunas para evitar erro de concatenação/schema
    if dfp.empty:
        dfp = pd.DataFrame(columns=[c for c in dfp.columns if c != 'listing_id'] + ['listing_id'])
    if dfm.empty:
        dfm = pd.DataFrame(columns=[c for c in dfm.columns if c != 'listing_id'] + ['listing_id'])
    if dfa.empty:
        dfa = pd.DataFrame(columns=['listing_id', 'amenity_raw', 'amenity'])

    dfp.to_parquet(os.path.join(outdir, "silver_pricing.parquet"), engine="pyarrow", index=False)
    dfm.to_parquet(os.path.join(outdir, "silver_medias.parquet"), engine="pyarrow", index=False)
    dfa.to_parquet(os.path.join(outdir, "silver_amenities.parquet"), engine="pyarrow", index=False)

    print("✅ Silver gerado em:", outdir)
    print(" - silver_listings.parquet:", len(dfl), "linhas")
    print(" - silver_pricing.parquet :", len(dfp), "linhas")
    print(" - silver_medias.parquet  :", len(dfm), "linhas")
    print(" - silver_amenities.parquet:", len(dfa), "linhas")


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Transformações essenciais da camada Silver (pandas).")
    ap.add_argument("--bronze", required=True, nargs="+",
                    help="Caminho(s) para Parquet do Bronze (aceita glob, ex: /lake/bronze/listings/*.parquet)")
    ap.add_argument("--outdir", required=True, help="Diretório de saída para as tabelas Silver (Parquet)")
    args = ap.parse_args()

    build_silver_tables(args.bronze, args.outdir)


if __name__ == "__main__":
    main()