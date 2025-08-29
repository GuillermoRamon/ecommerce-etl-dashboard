import pandas as pd
from etl.utils.constants import NOT_DEFINED
from etl.utils.validate import *


def unify(df_es: pd.DataFrame, df_uk:pd.DataFrame) -> pd.DataFrame:

    # required
    check_required_columns(df_es,["codigo","nombre"], "ES")
    check_required_columns(df_uk,["code","name"], "UK")

    # rename
    df_es = df_es.rename(columns={"codigo":"category_code","nombre":"category_name_es"})
    df_uk = df_uk.rename(columns={"code":"category_code","name":"category_name_uk"})

    # format
    check_category_code_format(df_es, "category_code","ES")
    check_category_code_format(df_uk, "category_code","UK")

    # unify & checks
    check_duplicates(df_es,"category_code","ES")
    check_duplicates(df_uk,"category_code","UK")
    
    df = pd.merge(df_es,df_uk, on="category_code", how="outer").fillna(NOT_DEFINED).astype(str).apply(lambda col: col.str.strip())
    
    check_duplicates(df,"category_code","GLOBAL")

    return df
