from __future__ import annotations
from pathlib import Path
import pandas as pd
from etl.core.customers import unify


def run(input_es: str | Path, input_uk: str | Path, out_path: str | Path) -> dict:

    """
    Step to unify Spanish and UK customers:
    - Validates required columns 
    - Standardizes column names to a common schema
    - Normalizes country to country_code (ES/UK), lowercases emails, and parses dates
    - Generates global_customer_id to avoid collisions across sources
    - Validates duplicates
    """

    df_es = pd.read_csv(input_es, dtype={"codigo_postal":"string"})
    df_uk = pd.read_csv(input_uk, dtype={"postal_code":"string"})

    df_unified = unify(df_es, df_uk)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_unified.to_parquet(out_path, index=False)

    return {
        "rows": int(df_unified.shape[0]),
        "cols": int(df_unified.shape[1]),
        "path": str(out_path)
    }