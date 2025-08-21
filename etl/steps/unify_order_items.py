from __future__ import annotations
from pathlib import Path
import pandas as pd
from etl.core.order_items import unify


def run(input_es: str | Path, input_uk: str | Path, out_path: str | Path) -> dict:
    
    """
    Step to unify Spanish and UK order_items:
    - Validates required columns
    - Standardizes column names to a common schema
    - Normalizes price formats and converts to EUR
    - Generates line_number
    - Adds source_system (ES/UK)
    - Validates duplicates 
    """

    df_es = pd.read_csv(input_es)
    df_uk = pd.read_csv(input_uk)

    df_unified = unify(df_es, df_uk)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_unified.to_parquet(out_path, index=False)

    return {
        "rows": int(df_unified.shape[0]),
        "cols": int(df_unified.shape[1]),
        "path": str(out_path)
    }