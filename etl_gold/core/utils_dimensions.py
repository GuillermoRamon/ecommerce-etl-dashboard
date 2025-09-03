from pyspark.sql import DataFrame, functions as F 

def create_dimension(business_key: str, surrogate_col:str, cols: list[str], df: DataFrame) -> DataFrame:
    
    df_sel = (
        df.select(*cols)
        .filter(F.col(business_key).isNotNull())
        )

    df_dedup = df_sel.dropDuplicates([business_key])

    df_dim = (
        df_dedup
          .withColumn(surrogate_col, F.xxhash64(F.col(business_key)))
          .select(
              surrogate_col,
              *cols
          )
    )

    return df_dim

def write_dimension(df:DataFrame, out_path: str) -> dict:

    (df.repartition(1)
    .write
    .mode("overwrite")
    .parquet(out_path))

    return{
        "path": out_path,
        "rows": df.count(),
        "cols": len(df.columns)
    }