import pandas as pd

EXCHANGE_RATES = {
    "EUR": 1.0,
    "GBP": 1.15   # 1 GBP = 1.15 EU
}

def price_format(df: pd.DataFrame, column:str, source_system: str) -> pd.Series:

    """
    Normalize a price column to float64 in-place.
    - ES: remove thousand separator (.) and replace decimal comma (,) with dot (.)
    - UK/others: remove thousand separator (,)
    """

    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    s = df[column].astype(str).str.strip()

    if source_system.upper() == "ES":
        s = (
            s.str.replace(".", "", regex=False) # thousands
            .str.replace(",", ".", regex=False) # decimal comma
            ) 
    else:
        s = s.str.replace(",", "", regex=False)
    
    df[column] = pd.to_numeric(s, errors="coerce").astype("float64")
    return df[column]

def to_euro(df: pd.DataFrame, column:str, currency: str) -> pd.Series:

    """
    Convert the numeric column to EUR using EXCHANGE_RATES.
    """

    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")
    
    cur = currency.upper()
    
    if cur not in EXCHANGE_RATES:
        raise ValueError(f"Currency '{currency}' not supported. Update EXCHANGE_RATES.")
    
    df["unit_price_eur"] = df[column] * EXCHANGE_RATES[cur]

    return df["unit_price_eur"]
    
