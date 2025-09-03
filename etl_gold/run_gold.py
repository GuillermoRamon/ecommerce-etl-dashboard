import yaml
import importlib, time, json
from pathlib import Path
from pyspark.sql import SparkSession

STEPS = [
    "etl_gold.steps.build_dim_customer",
    "etl_gold.steps.build_dim_category",
    "etl_gold.steps.build_dim_product"
]

def build_spark(app_name: str = "ecommerce-gold"):
    return (SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
        )

def load_cfg():
    yml_path = Path(__file__).parent / "config" / "gold.yaml"
    with open(yml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    spark = build_spark()
    cfg = load_cfg()

    for s in STEPS:
        s_name = s.split(".")[-1]
        mod = importlib.import_module(s)

        t0 = time.time()
        res = mod.run(spark,cfg)
        res["seconds"] = round(time.time() - t0, 2)

        print(f"[OK] {s_name}: {json.dumps(res, ensure_ascii=False)}")


    spark.stop()

if __name__ == "__main__":
    main()