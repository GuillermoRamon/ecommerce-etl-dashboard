from pyspark.sql import functions as F 
from etl_gold.core.utils_dimensions import create_dimension,write_dimension

def run(spark, cfg):
    path = cfg["inputs"]["products_parquet"]
    out_path = cfg["outputs"]["dim_product_parquet"]
    df = spark.read.parquet(path)

    cols = [
        "product_code", "category_code_es", "product_name_es",
        "supplier_es", "unit_price_es", "category_code_uk",  
        "product_name_uk", "supplier_uk", "unit_price_uk"
    ]

    df_dim = create_dimension("product_code","product_sk",cols, df)

    return {
        "table": "dim_product",
        **write_dimension(df_dim, out_path)
    }