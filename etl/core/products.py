import pandas as pd
from etl.utils.constants import NOT_DEFINED
from etl.utils.validate import *
from etl.utils.currency import price_format

def unify(df_es: pd.DataFrame, df_uk: pd.DataFrame) -> pd.DataFrame:

    # required
    check_required_columns(df_es,["codigo","codigo_categoria","nombre","proveedor","precio_unitario"], "ES")
    check_required_columns(df_uk,["code","category_code","name","supplier","unit_price"], "UK")

    # rename
    df_es = df_es.rename(columns={"codigo":"product_code","codigo_categoria":"category_code","nombre":"product_name_es","proveedor":"supplier_es","precio_unitario":"unit_price_es"})
    df_uk = df_uk.rename(columns={"code":"product_code","name":"product_name_uk","supplier":"supplier_uk","unit_price":"unit_price_uk"})

    # format
    check_products_code_format(df_es, "product_code","ES")
    check_products_code_format(df_uk, "product_code","UK")

    df_es["unit_price_es"] = price_format(df_es,"unit_price_es","ES")
    df_uk["unit_price_uk"] = price_format(df_uk,"unit_price_uk","UK")

    # checks
    check_duplicates(df_es,"product_code","ES")
    check_duplicates(df_uk,"product_code","UK")

    # unify
    df = pd.merge(df_es,df_uk, on=["product_code"], how="outer").fillna(NOT_DEFINED)

    # order & check
    str_cols = ["product_name_es","supplier_es","product_name_uk","supplier_uk"]
    df[str_cols] = df[str_cols].astype(str).apply(lambda col: col.str.strip())

    check_duplicates(df,"product_code","GLOBAL")

    return df
    