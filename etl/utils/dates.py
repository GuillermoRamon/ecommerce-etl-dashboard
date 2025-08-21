import pandas as pd

DAYFIRST_BY_COUNTRY = {
    "ES": True,
    "UK": False
}

def date_format(df: pd.DataFrame, column: str, origin: str, not_date: bool = False) -> pd.DataFrame:

    """
    - Provide a common datetime format for columns.
    - If a value cannot be parsed and `not_date=True`, set it to 1700-01-01.
    """

    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")

    if origin is None:
        raise ValueError("You must provide 'origin' (e.g., 'ES' or 'UK')")

    key = str(origin).upper().strip()
    if key not in DAYFIRST_BY_COUNTRY:
        raise ValueError(f"Origin '{origin}' not supported. Configure DAYFIRST_BY_COUNTRY.")

    day = DAYFIRST_BY_COUNTRY[key]
    df[column] = pd.to_datetime(df[column], errors="coerce", dayfirst=day)
    if not_date:
        df[column] = df[column].fillna(pd.Timestamp("1700-01-01"))
    return df
        
