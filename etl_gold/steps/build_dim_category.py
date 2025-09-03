from pyspark.sql import functions as F 
from etl_gold.core.utils_dimensions import create_dimension,write_dimension

def run(spark, cfg):
    path = cfg["inputs"]["categories_parquet"]
    out_path = cfg["outputs"]["dim_category_parquet"]
    df = spark.read.parquet(path)

    cols = [
        "category_code", "category_name_es", "category_name_uk"
    ]

    df_dim = create_dimension("category_code","category_sk",cols, df)

    return {
        "table": "dim_category",
        **write_dimension(df_dim, out_path)
    }