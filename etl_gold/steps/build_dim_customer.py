from pyspark.sql import functions as F 
from etl_gold.core.utils_dimensions import create_dimension,write_dimension

def run(spark, cfg):
    path = cfg["inputs"]["customers_parquet"]
    out_path = cfg["outputs"]["dim_customer_parquet"]
    df = spark.read.parquet(path)

    cols = [
        "global_customer_id", "customer_id", "gov_id",
        "customer_name", "email","country", "region",
        "city", "postal_code", "address","registration_date",
        "activation_date", "deactivation_date"
    ]

    df_dim = create_dimension("global_customer_id","customer_sk",cols, df)

    return {
        "table": "dim_customer",
        **write_dimension(df_dim, out_path)
    }
