import pandas as pd
from pathlib import Path
from datetime import datetime

"""
Validation utilities for ETL steps.
Includes checks for duplicates, required columns, ID formats, and date consistency.
"""

def check_duplicates(df: pd.DataFrame, column: str, origen: str = ''):
    if df[column].duplicated().any():
        raise ValueError(f"Duplicate values found in {origen}-{column}")

def check_required_columns(df: pd.DataFrame, required_cols: list[str], origen: str = ''):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {origen}: {missing}")
    
def check_category_code_format(df: pd.DataFrame, column: str, origen: str = ''):
    pattern = r"^C\d{3}$"
    invalid = df[~df[column].astype(str).str.match(pattern, na=False)]
    if not invalid.empty:
        raise ValueError(f"Invalid category code format in {origen}: {invalid}")
    
def check_products_code_format(df: pd.DataFrame, column: str, origen: str=''):
    pattern = r"^P\d{5}$"
    invalid = df[~df[column].astype(str).str.match(pattern, na=False)]
    if not invalid.empty:
        raise ValueError(f"Invalid product code format in {origen}: {invalid}")
    
def check_gov_id_format(df: pd.DataFrame, column: str, origen: str=''):
    pattern = r'^(?:\d{8}[A-Z]|[A-Z]{2}\d{6}[A-Z])$'
    invalid = df[~df[column].astype(str).str.match(pattern, na=False)]
    if not invalid.empty:
        raise ValueError(f"Invalid gov_id format in {origen}: {invalid}")
    
def check_order_date(df: pd.DataFrame, order: str, ship: str, origen: str = ''):
        
    both = df[order].notna() & df[ship].notna()
    bad = df.loc[both & (df[order] > df[ship])] 
    if not bad.empty:
        raise ValueError(f"Invalid order/ship date in {origen}: order after ship")
    
def check_ship_delay(df: pd.DataFrame, order: str, ship: str, max_days: int, origen: str = "", dir: str | Path = "logs"):
        
        both = df[order].notna() & df[ship].notna()
        delay = (df.loc[both, ship] - df.loc[both, order]).dt.days
        too_long = delay[delay > max_days]

        if not too_long.empty:
            warnings_dir = Path(dir)
            warnings_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = warnings_dir / f"warnings_orders_{origen}_{ts}.txt"

            with open(fname, "w", encoding="utf-8") as f:
                f.write(f"[WARN_Check: {too_long.shape[0]} delayed shipments\n")
                f.write(f"Generated: {datetime.now().isoformat()}\n\n")
                df.loc[too_long.index, [order, ship]].to_string(f, index=False)

            print(f"[WARN_Check {too_long.shape[0]} cases exceeded max allowed delay. Saved to {fname}")