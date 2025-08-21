import uuid
import pandas as pd

NAMESPACE = uuid.UUID("00000000-0000-0000-0000-000000000001")

def make_id(df: pd.DataFrame,column: str, source_system: str) -> pd.Series:

    """
    Generate a deterministic global ID from a column and source system.

    - Combines source_system and column value as key.
    - Uses UUID5 with a fixed namespace.
    - Returns a 12-character hex string.
    """

    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    keys = (
        str(source_system).strip().upper() + ":" +
        df[column].astype(str).str.strip().str.lower()
    )
    
    return keys.apply(lambda x:uuid.uuid5(NAMESPACE,x).hex[:12])
