from pyspark.sql import functions as F 
from etl_gold.core.utils_dimensions import write_dimension, load_ref_yaml, ref_by_code

def run(spark, cfg):
    path = cfg["inputs"]["orders_parquet"]
    out_path = cfg["outputs"]["dim_shipping_method_parquet"]

    df = spark.read.parquet(path)
    ref_df = load_ref_yaml(spark, "ref_shipping_methods.yaml", "shipping_methods")

    df_dim = ref_by_code("shipping_method_id", "shipping_method_sk",df, ref_df)

    df_dim = (
        df_dim
        .withColumnRenamed("shipping_method_id", "shipping_method_code")
        .drop("code")
    )
    
    return {
        "table": "dim_shipping_method",
        **write_dimension(df_dim, out_path)
    }