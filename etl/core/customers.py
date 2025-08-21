import pandas as pd
from etl.utils.constants import NOT_DEFINED
from etl.utils.validate import *
from etl.utils.dates import date_format
from etl.utils.ids import make_id


def unify(df_es: pd.DataFrame, df_uk:pd.DataFrame) -> pd.DataFrame:

    # required
    check_required_columns(df_es,["id_cliente","id_oficial","nombre","correo","pais",
                                  "region","ciudad","codigo_postal","direccion",
                                  "fecha_registro","fecha_baja","fecha_alta"], "ES")
    check_required_columns(df_uk,["customer_id","gov_id","name","email","country",
                                  "region","city","postal_code","address",
                                  "registration_date","deactivation_date","activation_date"], "UK")

    # rename
    df_es = df_es.rename(columns={"id_cliente":"customer_id","id_oficial":"gov_id",
                                  "nombre":"customer_name","correo":"email",
                                  "pais":"country","ciudad":"city","codigo_postal":"postal_code",
                                  "direccion":"address","fecha_registro":"registration_date",
                                  "fecha_baja":"deactivation_date","fecha_alta":"activation_date"})
    df_uk = df_uk.rename(columns={"name":"customer_name"})

    # format
    check_gov_id_format(df_es, "gov_id","ES")
    check_gov_id_format(df_uk, "gov_id","UK")

    for d in (df_es, df_uk):
        d["email"] = d["email"].astype(str).str.strip().str.lower()

    df_es["country"] = df_es["country"].str.strip().str.upper().replace("SPAIN", "ES")

    # duplicates
    check_duplicates(df_es, "customer_id", "ES")
    check_duplicates(df_uk, "customer_id", "UK")
    check_duplicates(df_es,"gov_id","ES")
    check_duplicates(df_uk,"gov_id","UK")

    # dates
    df_es = date_format(df_es,"registration_date","ES")
    df_es = date_format(df_es,"deactivation_date","ES",True)
    df_es = date_format(df_es,"activation_date","ES",True)

    df_uk = date_format(df_uk,"registration_date","UK")
    df_uk = date_format(df_uk,"deactivation_date","UK",True)
    df_uk = date_format(df_uk,"activation_date","UK",True)
    
    # global customer id 
    df_es["global_customer_id"] = make_id(df_es,"customer_id","ES")
    df_uk["global_customer_id"] = make_id(df_uk,"customer_id","UK")
    
    # unify 
    df = pd.concat([df_es, df_uk], ignore_index=True)

    # order & checks
    df.insert(0, "global_customer_id", df.pop("global_customer_id"))
    check_duplicates(df,"global_customer_id","GLOBAL")

    return df