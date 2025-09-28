from pyspark.sql import functions as F 
from etl_gold.core.utils_dimensions import write_dimension, load_ref_yaml, ref_with_norm

def run(spark, cfg):
    path = cfg["inputs"]["orders_parquet"]
    out_path = cfg["outputs"]["dim_payment_type_parquet"]

    df = spark.read.parquet(path)

    ref_df = load_ref_yaml(spark, "ref_payment_type.yaml", "payment_type")

    df_dim = ref_with_norm(
        df=df,
        src_col="payment_type",
        ref_df=ref_df,
        sk_col="payment_type_sk",
        name_col="payment_type_name",
        code_col="payment_type_code"
    )
    
    return {
        "table": "dim_payment_type",
        **write_dimension(df_dim, out_path)
    }