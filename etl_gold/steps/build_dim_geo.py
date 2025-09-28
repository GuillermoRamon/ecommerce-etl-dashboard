from pyspark.sql import functions as F
from etl_gold.core.utils_dimensions import create_dimension,write_dimension

def run(spark, cfg):
    path = cfg["inputs"]["customers_parquet"]
    out_path = cfg["outputs"]["dim_geo_parquet"]
    df = spark.read.parquet(path)

    cols = ["country", "region","city","geo_nk"]

    df_geo = df.select(
        "country",
        "region",
        "city",
        F.concat_ws("|", F.col("country"), F.col("city")).alias("geo_nk")
    )

    df_dim = create_dimension("geo_nk", "geo_sk", cols, df_geo) \
            .drop("geo_nk")

    return {
        "table": "dim_geo",
        **write_dimension(df_dim, out_path)
    }