import re
import os
import json
import argparse
from datetime import datetime, timezone
from typing import List, Optional, Any
import numpy as np
import warnings
import pandas as pd
from bs4 import BeautifulSoup
from bs4 import MarkupResemblesLocatorWarning
from dateutil import parser as dtparser

# Silencia apenas o aviso específico do BeautifulSoup (sem calar o resto)
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

# -------------------------------------------------------------------------------------------------
# Colunas de data originais. MANTIDAS NO FORMATO ORIGINAL para não quebrar a lógica
# de procura no bronze_ingest, já que o snake_case foi removido.
_URLISH_RE = re.compile(r"^https?://", re.IGNORECASE)
DATA_COL = [
    "createdAt",
    "updatedAt",
    "deliveredAt",
    "account.createdDate",
]


def unwanted_character(df: pd.DataFrame) -> pd.DataFrame:

    def _is_list_like(v: Any) -> bool:
        return isinstance(v, (list, tuple, set, np.ndarray, pd.Series))

    def clean_text(v: Any) -> Any:
        # Se for uma estrutura iterável (lista/tuple/ndarray/Series), aplica recursivamente
        if _is_list_like(v):
            # converte pd.Series para lista para iterar, depois reconstrói o tipo original
            seq = list(v) if not isinstance(v, list) else v
            cleaned = [clean_text(x) for x in seq]
            if isinstance(v, tuple):
                return tuple(cleaned)
            if isinstance(v, set):
                return set(cleaned)
            if isinstance(v, np.ndarray):
                return np.array(cleaned, dtype=object)
            # pd.Series ou list — devolve lista simples (mantemos tipo list para coerência)
            if isinstance(v, pd.Series):
                return pd.Series(cleaned, index=v.index)
            return cleaned  # list

        # Se for dict, limpa valores recursivamente
        if isinstance(v, dict):
            return {k: clean_text(val) for k, val in v.items()}

        # Agora é seguro usar pd.isna porque v é (esperadamente) escalar
        if pd.isna(v):
            return v

        txt = str(v)

        # URLs / scheme-like: não passar pelo BeautifulSoup
        if not _URLISH_RE.match(txt):
            txt = BeautifulSoup(txt, "html.parser").get_text()

        # remove aspas e colchetes literais
        txt = re.sub(r'[\"\[\]\']', "", txt)
        return txt.strip()

    # Faz uma cópia para evitar SettingWithCopyWarning
    df_cleaned = df.copy()

    for col in df_cleaned.columns:
        s = df_cleaned[col]

        # Aplica a limpeza e garante que s_cleaned é Series 1-D com mesmo índice
        s_cleaned = pd.Series(s.apply(clean_text), index=s.index, dtype="object")

        # Substitui strings vazias por NaN para ajudar a conversão numérica
        s_for_numeric = s_cleaned.replace("", np.nan)

        # tenta conversão numérica (coerce)
        try:
            s_num = pd.to_numeric(s_for_numeric, errors="coerce")
        except TypeError:
            # se to_numeric reclamar, mantém o texto limpo
            df_cleaned[col] = s_cleaned
            continue

        # Decide se usa a versão numérica ou textual
        if s_num.notna().any():
            mask_num = s_num.notna()
            # compara representações somente onde há número
            str_cleaned = s_cleaned.astype(object).astype(str)
            str_num = s_num.astype(object).astype(str)
            if (str_cleaned[mask_num] != str_num[mask_num]).any():
                df_cleaned[col] = s_num
                continue

        df_cleaned[col] = s_cleaned

    return df_cleaned

def standardization_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza o nome das colunas, mantendo apenas o último nome.
    Se houver duplicatas (ex: listing.id e account.id virando 'id'),
    adiciona um sufixo numérico (_1, _2) para evitar erro.
    """
    df = df.copy()
    new_cols = []
    seen = {} # Dicionário para rastrear nomes repetidos

    for c in df.columns:
        # Pega o último elemento após o último ponto
        base_name = c.split('.')[-1]

        if base_name in seen:
            seen[base_name] += 1
            # Cria nome único: id_1, id_2, etc.
            final_name = f"{base_name}_{seen[base_name]}"
        else:
            seen[base_name] = 0
            final_name = base_name

        new_cols.append(final_name)

    df.columns = new_cols
    return df

    """Normaliza o nome das colunas, mantendo apenas o último nome (após o último '.')."""
    df = df.copy()
    # Pega o último elemento após o último ponto. Ex: 'a.b.c' -> 'c'
    # Se não houver ponto, retorna o nome original.
    df.columns = [c.split('.')[-1] for c in df.columns]
    return df

def to_ts_or_none(x: Any) -> Optional[pd.Timestamp]:
    """Converte valor 'qualquer' em Timestamp UTC ou None, por elemento (sem avisos)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if not s:
        return None
    # Tenta parsing direto com dateutil (por elemento) e normaliza para UTC
    try:
        dt = dtparser.parse(s)
    except Exception:
        return None
    try:
        return pd.Timestamp(dt).tz_localize("UTC") if dt.tzinfo is None else pd.Timestamp(dt).tz_convert("UTC")
    except Exception:
        # fallback robusto
        try:
            return pd.to_datetime(dt, utc=True)
        except Exception:
            return None

def coerce_jsonish(txt: Any) -> Optional[str]:
    """
    Normaliza pseudo-JSON:
      - aspas simples -> duplas
      - True/False -> true/false
      - None -> null
    Retorna string JSON válida ou None.
    """
    if txt is None or (isinstance(txt, float) and pd.isna(txt)):
        return None
    t = str(txt).strip()
    if not t or not (t.startswith("[") or t.startswith("{")):
        return None
    t = re.sub(r"\bTrue\b", "true", t)
    t = re.sub(r"\bFalse\b", "false", t)
    t = re.sub(r"\bNone\b", "null", t)
    t = t.replace("'", '"')
    return t

# Adicione esta função junto com as outras (parse_medias, etc)
def parse_strings_list(cell: Any) -> Optional[List[str]]:
    j = coerce_jsonish(cell) # Usa sua função existente para arrumar aspas
    if not j:
        return None
    try:
        data = json.loads(j)
        if isinstance(data, list):
            return [str(x) for x in data] # Garante que são strings
    except Exception:
        return None
    return None

def parse_pricing_infos(cell: Any) -> Optional[List[dict]]:
    j = coerce_jsonish(cell)
    if not j:
        return None
    try:
        data = json.loads(j)
    except Exception:
        return None

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return None

    out: List[dict] = []
    for d in data:
        if not isinstance(d, dict):
            continue
        ri = d.get("rentalInfo")
        if isinstance(ri, dict):
            ri = {
                "period": str(ri.get("period")) if ri.get("period") is not None else None,
                "warranties": [str(w) for w in (ri.get("warranties") or [])],
                "monthlyRentalTotalPrice": (
                    str(ri.get("monthlyRentalTotalPrice"))
                    if ri.get("monthlyRentalTotalPrice") is not None else None
                ),
            }
        else:
            ri = None

        out.append({
            "iptuPeriod":        str(d.get("iptuPeriod")) if d.get("iptuPeriod") is not None else None,
            "rentalInfo":        ri,
            "yearlyIptu":        str(d.get("yearlyIptu")) if d.get("yearlyIptu") is not None else None,
            "price":             str(d.get("price")) if d.get("price") is not None else None,
            "iptu":              str(d.get("iptu")) if d.get("iptu") is not None else None,
            "businessType":      str(d.get("businessType")) if d.get("businessType") is not None else None,
            "monthlyCondoFee":   str(d.get("monthlyCondoFee")) if d.get("monthlyCondoFee") is not None else None,
        })
    return out or None

def parse_medias(cell: Any) -> Optional[List[dict]]:
    j = coerce_jsonish(cell)
    if not j:
        return None
    try:
        data = json.loads(j)
    except Exception:
        return None

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return None

    out: List[dict] = []
    for d in data:
        if not isinstance(d, dict):
            continue
        out.append({
            "id":   str(d.get("id")) if d.get("id") is not None else None,
            "url":  str(d.get("url")) if d.get("url") is not None else None,
            "type": str(d.get("type")) if d.get("type") is not None else None,
        })
    return out or None

# -------------------------------------------------------------------------------------------------

def bronze_ingest(input_path: str, outdir: str) -> str:
    # Leitura robusta do CSV
    dataframe = pd.read_csv(
        input_path,
        sep=",",
        engine="python",
        on_bad_lines="skip",
        quotechar='"',
        escapechar="\\",
        encoding="utf-8",
    )

    # 1) Padroniza colunas (mantém nomes seguros)
    dataframe = standardization_columns(dataframe)

    # --- MUDANÇA AQUI: O PARSING VEM ANTES DA LIMPEZA ---
    
    # 2) Parsing de colunas JSON (Preço e Medias)
    # Primeiro verificamos se as colunas existem antes de tentar processar
    if "pricingInfos" in dataframe.columns:
        dataframe["pricinginfos_arr"] = dataframe["pricingInfos"].apply(parse_pricing_infos)
        dataframe["pricinginfos_json"] = dataframe["pricinginfos_arr"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else None
        )
    
    if "medias" in dataframe.columns:
        dataframe["medias_arr"] = dataframe["medias"].apply(parse_medias)
    
    for am_col in ["amenities", "mergedAmenities", "searchableAmenities"]:
        if am_col in dataframe.columns:
            # Cria, por exemplo, amenities_arr
            dataframe[f"{am_col}_arr"] = dataframe[am_col].apply(parse_strings_list)

    # 3) Agora sim, limpa caracteres indesejados (sem quebrar os JSONs que já salvamos nas colunas _arr)
    dataframe = unwanted_character(dataframe)

    # 4) Cria colunas de data *_ts
    ts_cols = {}
    for original_col in DATA_COL:
        col_name = original_col.split('.')[-1]
        if col_name in dataframe.columns:
            ts_cols[f"{col_name}_ts"] = dataframe[col_name].apply(to_ts_or_none)

    if ts_cols:
        dataframe = pd.concat([dataframe, pd.DataFrame(ts_cols, index=dataframe.index)], axis=1)

    # 5) Metadados
    dataframe["bronze_ingestion_ts"] = pd.Timestamp.now(tz="UTC")
    dataframe["bronze_source_file"] = os.path.abspath(input_path)

    # 6) Saída
    if not outdir.startswith("gs://"):
        os.makedirs(outdir, exist_ok=True)
    
    outpath = os.path.join(
        outdir,
        f"listings_bronze_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.parquet",
    )
    
    dataframe.to_parquet(outpath, engine="pyarrow", index=False)
    # dataframe.to_csv(outpath + ".csv", index=False, encoding="utf-8") # Opcional: Comentei para economizar espaço
    return outpath


    # Leitura robusta do CSV
    dataframe = pd.read_csv(
        input_path,
        sep=",",
        engine="python",          # necessário para on_bad_lines
        on_bad_lines="skip",
        quotechar='"',
        escapechar="\\",
        encoding="utf-8",
    )

    # 1) padroniza colunas primeiro (agora só pega o último nome, ex: 'lon')
    dataframe = standardization_columns(dataframe)

    # 2) limpa caracteres indesejados (sem warnings)
    dataframe = unwanted_character(dataframe)

    # 3) cria colunas *_ts para datas conhecidas. O nome da coluna (ex: 'createdAt')
    # deve ser procurado no novo df.columns (que agora só tem o último nome).
    ts_cols = {}
    for original_col in DATA_COL:
        # Pega o último nome da coluna original (ex: 'createdDate' de 'account.createdDate')
        col_name = original_col.split('.')[-1]
        if col_name in dataframe.columns:
            ts_cols[f"{col_name}_ts"] = dataframe[col_name].apply(to_ts_or_none)

    if ts_cols:
        dataframe = pd.concat([dataframe, pd.DataFrame(ts_cols, index=dataframe.index)], axis=1)

    # 4) parsing de colunas JSON-ish. Os nomes devem ser o último nome da coluna original
    # (ex: 'pricingInfos' de algo.pricingInfos).
    if "pricingInfos" in dataframe.columns: # Assumindo que 'pricingInfos' seja o último nome
        dataframe["pricinginfos_arr"] = dataframe["pricingInfos"].apply(parse_pricing_infos)
        dataframe["pricinginfos_json"] = dataframe["pricinginfos_arr"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else None
        )
    if "medias" in dataframe.columns: # Assumindo que 'medias' seja o último nome
        dataframe["medias_arr"] = dataframe["medias"].apply(parse_medias)

    # 5) metadados (sem deprecation)
    dataframe["bronze_ingestion_ts"] = pd.Timestamp.now(tz="UTC")
    dataframe["bronze_source_file"] = os.path.abspath(input_path)

    # 6) saída
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(
        outdir,
        f"listings_bronze_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.parquet",
    )
    dataframe.to_parquet(outpath, engine="pyarrow", index=False)
    dataframe.to_csv(outpath + ".csv", index=False, encoding="utf-8")
    return outpath

# -----------------------------
# Main (CLI)
# -----------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Ingestão Bronze (pandas) para listings CSV complicado."
    )
    ap.add_argument("--input", required=True, help="Caminho do CSV de entrada")
    ap.add_argument("--outdir", required=True, help="Diretório de saída (Parquet)")
    args = ap.parse_args()

    out = bronze_ingest(args.input, args.outdir)
    print(f"✅ Bronze gerado: {out}")

if __name__ == "__main__":
    main()