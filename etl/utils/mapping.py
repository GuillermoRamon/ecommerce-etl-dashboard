from etl.utils.constants import NOT_DEFINED

MAP_SHIPPING_ES = {
    "estandar": "SM001",
    "express": "SM002",
    "recogida": "SM003"
}

MAP_SHIPPING_UK = {
    "standard": "SM001",
    "express": "SM002",
    "pickup": "SM003"
}

def map_shipping_method(df, column, country):

    """
    Map shipping method names to standard IDs.

    - Uses ES or UK mapping depending on country.
    - Adds column 'shipping_method_id' with the mapped value.
    - Fills unknown methods with NOT_DEFINED.
    """
    
    mapping = MAP_SHIPPING_ES if country == "ES" else MAP_SHIPPING_UK

    df[column] = df[column].astype(str).str.strip().str.lower()

    df["shipping_method_id"] = df[column].map(mapping).fillna(NOT_DEFINED)

    return df
