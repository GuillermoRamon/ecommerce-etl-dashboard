from pyspark.sql import functions as F 
from etl_gold.core.utils_dimensions import create_dimension,write_dimension


def run(spark, cfg, start_date: str = "2020-01-01",end_date: str = "2030-12-31"):
    
    out_path = cfg["outputs"]["dim_date_parquet"]

    start_col = F.lit(start_date).cast("date")
    end_col   = F.lit(end_date).cast("date")
    days_diff = (
        spark.range(1)
            .select(F.datediff(end_col, start_col).alias("d"))
            .first()["d"]
    )

    df = (spark.range(1)
        .select(F.sequence(start_col, end_col, F.expr("interval 1 day")).alias("date_seq"))
        .select(F.explode("date_seq").alias("date"))
    )
    
    df_dim = (
        df.withColumn("date", F.to_date(F.col("date")))
        .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("day_of_week", F.date_format("date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("is_weekend", (F.col("day_of_week").isin("Saturday", "Sunday")))
    )

    cols = df_dim.columns
    df_dim = df_dim.select(["date_sk"] + [c for c in cols if c != "date_sk"])

    return {
        "table": "dim_date",
        **write_dimension(df_dim, out_path)
    }