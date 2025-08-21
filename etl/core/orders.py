import pandas as pd
from etl.utils.constants import NOT_DEFINED
from etl.utils.validate import *
from etl.utils.dates import date_format
from etl.utils.mapping import map_shipping_method
from etl.utils.ids import make_id


def unify(df_es: pd.DataFrame, df_uk:pd.DataFrame) -> pd.DataFrame:

    # required
    check_required_columns(df_es,["id_pedido","id_cliente","fecha_pedido","fecha_envio","metodo_pago","estado_pedido","metodo_envio"], "ES")
    check_required_columns(df_uk,["id","customer_id","order_date","ship_date","payment_type","status","delivery_method"], "UK")

    # rename
    df_es = df_es.rename(columns={
        "id_pedido":"order_id_source","id_cliente":"customer_id_source",
        "fecha_pedido":"order_date","fecha_envio":"ship_date",
        "metodo_pago":"payment_type","estado_pedido":"status","metodo_envio":"delivery_method"})
    df_uk = df_uk.rename(columns={"id":"order_id_source","customer_id":"customer_id_source"})

    # new column
    df_es["source_system"] = "ES"
    df_uk["source_system"] = "UK"
 
    # dates
    df_es = date_format(df_es,"order_date","ES")
    df_uk = date_format(df_uk,"order_date","UK")
    df_es = date_format(df_es,"ship_date","ES")
    df_uk = date_format(df_uk,"ship_date","UK")

    # checks
    check_duplicates(df_es,"order_id_source","ES")
    check_duplicates(df_uk,"order_id_source","UK")
    check_order_date(df_es, "order_date","ship_date", "ES")
    check_order_date(df_uk, "order_date","ship_date", "UK")
    check_ship_delay(df_es, "order_date", "ship_date", max_days=20, origen="ES")
    check_ship_delay(df_uk, "order_date", "ship_date", max_days=20, origen="UK")

    # format
    df_es = map_shipping_method(df_es,"delivery_method","ES")
    df_uk = map_shipping_method(df_uk,"delivery_method","UK")

    # ids
    df_es["global_order_id"] = make_id(df_es,"order_id_source","ES")
    df_uk["global_order_id"] = make_id(df_uk,"order_id_source","UK")

    df_es["global_customer_id"] = make_id(df_es,"customer_id_source","ES")
    df_uk["global_customer_id"] = make_id(df_uk,"customer_id_source","UK")
    
    # unify
    df = pd.concat([df_es, df_uk], ignore_index=True)

    # order & checks
    df.insert(0, "global_customer_id", df.pop("global_customer_id"))
    df.insert(0, "global_order_id", df.pop("global_order_id"))
    
    check_duplicates(df,"global_order_id","GLOBAL")

    return df