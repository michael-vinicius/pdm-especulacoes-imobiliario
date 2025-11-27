import argparse
import pandas as pd
import os

def join_listings_pricing(silver_path: str,
                          out_path: str,
                          business_type: str = "sale",
                          price_col_fallback: str = "price") -> str:
    """
    Junta listings + pricing da camada Silver e salva em Parquet.
    """
    # lê dataframes
    df_listings = pd.read_parquet(os.path.join(silver_path, "silver_listings.parquet"))
    df_pricing  = pd.read_parquet(os.path.join(silver_path, "silver_pricing.parquet"))

    biz = business_type.lower()

    # escolhe coluna de preço
    if biz == "sale":
        price_col = "price" if "price" in df_pricing.columns else price_col_fallback
    else:
        price_col = "monthly_total_rent" if "monthly_total_rent" in df_pricing.columns else price_col_fallback

    # filtra por tipo de negócio (se existir a coluna)
    if "business_type" in df_pricing.columns:
        df_pricing = df_pricing[df_pricing["business_type"].str.lower() == biz]

    # remove NaN em preço
    df_pricing = df_pricing.dropna(subset=[price_col])

    # uma linha por listing
    df_pricing = (
        df_pricing.sort_values("listing_id")
                  .drop_duplicates("listing_id", keep="first")
                  .rename(columns={price_col: "target_price"})
                  [["listing_id", "target_price"]]
    )

    # join
    df_joined = df_listings.merge(df_pricing, on="listing_id", how="inner")

    # salva
    # Só tenta criar pasta se NÃO for nuvem
    if not out_path.startswith("gs://"):
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df_joined.to_parquet(out_path, index=False)

    return out_path


def main():
    ap = argparse.ArgumentParser(description="Join Silver listings + pricing em único Parquet")
    ap.add_argument("--silver", required=True, help="Diretório onde estão os Parquets da camada Silver")
    ap.add_argument("--out", required=True, help="Caminho do arquivo Parquet de saída")
    ap.add_argument("--business-type", default="sale", choices=["sale", "rental"], help="Tipo de negócio (sale|rental)")
    args = ap.parse_args()

    out_file = join_listings_pricing(args.silver, args.out, business_type=args.business_type)
    print(f"✅ Silver join listings+pricing salvo em: {out_file}")


if __name__ == "__main__":
    main()
