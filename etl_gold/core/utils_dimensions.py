from pyspark.sql import DataFrame, functions as F 
import yaml
import pandas as pd
from pathlib import Path

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

def load_ref_yaml(spark, filename, key):

    base_path = Path(__file__).resolve().parent.parent / "config"
    file_path = base_path / filename

    with open(file_path, "r", encoding="utf-8") as f:
        ref_dict = yaml.safe_load(f)
    ref_df = pd.DataFrame(ref_dict[key])
    return spark.createDataFrame(ref_df)


def ref_with_norm(
    df: DataFrame, 
    src_col: str,             
    ref_df: DataFrame,        
    sk_col: str,              
    name_col: str,            
    code_col: str             
) -> DataFrame:
    
    df_norm = df.withColumn("src_norm", F.lower(F.trim(F.col(src_col))))

    ref_exploded = ref_df.withColumn("norm", F.explode(F.col("normalize")))

    df_joined = (
        df_norm.join(ref_exploded, df_norm["src_norm"] == ref_exploded["norm"], "left")
    )

    df_dim = (
        df_joined
        .withColumn(sk_col, F.xxhash64(F.col("name")))
        .select(sk_col, F.col("code").alias(code_col), F.col("name").alias(name_col))
        .dropDuplicates()
    )
    return df_dim

def ref_by_code(business_key, surrogate_col, df, ref_df):

    df_sel = (
    df.select(business_key)
    .filter(F.col(business_key).isNotNull())
    .dropDuplicates([business_key])
    )
    
    df_joined = df_sel.join(ref_df, df_sel[business_key] == ref_df["code"], how="left")

    cols = df_joined.columns

    df_dim = (
        df_joined
          .withColumn(surrogate_col, F.xxhash64(F.col(business_key)))
          .select(
              surrogate_col,
              *cols
          )
    )

    return df_dim
