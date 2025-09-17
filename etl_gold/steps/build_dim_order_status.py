from pyspark.sql import functions as F 
from etl_gold.core.utils_dimensions import write_dimension, load_ref_yaml, ref_with_norm

def run(spark, cfg):
    path = cfg["inputs"]["orders_parquet"]
    out_path = cfg["outputs"]["dim_order_status_parquet"]

    df = spark.read.parquet(path)

    ref_df = load_ref_yaml(spark, "ref_order_status.yaml","order_status")

    df_dim = ref_with_norm(df, "status", ref_df, "order_status_sk", "order_status_name", "order_status_code")
    
    return {
        "table": "dim_order_status",
        **write_dimension(df_dim, out_path)
    }