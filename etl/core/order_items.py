import pandas as pd
from etl.utils.constants import NOT_DEFINED
from etl.utils.validate import *
from etl.utils.ids import make_id
from etl.utils.currency import price_format,to_euro



def unify(df_es: pd.DataFrame, df_uk:pd.DataFrame) -> pd.DataFrame:

    # required
    check_required_columns(df_es,["id_pedido","codigo_producto","cantidad","precio_unitario","devuelto"], "ES")
    check_required_columns(df_uk,["order_id","product_code","quantity","unit_price","returned"], "UK")

    # rename
    df_es = df_es.rename(columns={"id_pedido":"order_id","codigo_producto":"product_code",
                                  "cantidad":"quantity","precio_unitario":"original_unit_price",
                                  "devuelto":"returned"})
    df_uk = df_uk.rename(columns={"unit_price":"original_unit_price"})

    # new id
    df_es["order_id"] = make_id(df_es,"order_id","ES")
    df_uk["order_id"] = make_id(df_uk,"order_id","UK")

    # new column
    df_es["source_system"] = "ES"
    df_uk["source_system"] = "UK"

    # format
    df_es["original_unit_price"] = price_format(df_es,"original_unit_price","ES")
    df_es["unit_price_eur"] = df_es["original_unit_price"].astype("float64").round(2)

    df_uk["original_unit_price"] = price_format(df_uk,"original_unit_price","UK")
    df_uk["unit_price_eur"] = to_euro(df_uk, "original_unit_price", "GBP").round(2)

    # unify
    df = pd.concat([df_es, df_uk], ignore_index=True)

    # new column
    df["line_number"] = df.groupby("order_id").cumcount() + 1 # sequential per order
    cols = ["line_number"] + [c for c in df.columns if c != "line_number"]
    df = df[cols]

    # type
    df["quantity"] = df["quantity"].astype("int32")
    df["line_number"] = df["line_number"].astype("int32")

    #check
    check_duplicates(df, ["order_id", "line_number"], "GLOBAL")

    return df