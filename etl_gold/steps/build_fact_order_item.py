from etl_gold.core.utils_dimensions import load_ref_yaml,write_dimension,apply_ref_mapping
from pyspark.sql import functions as F

def run(spark, cfg):

    orders = spark.read.parquet(cfg["inputs"]["orders_parquet"])
    items = spark.read.parquet(cfg["inputs"]["order_items_parquet"])

    out_path = cfg["outputs"]["fact_order_item_parquet"]

    dim_paths = {
        "date": "dim_date_parquet",
        "customer": "dim_customer_parquet",
        "product": "dim_product_parquet",
        "shipping": "dim_shipping_method_parquet",
        "payment": "dim_payment_type_parquet",
        "category":"dim_category_parquet",
        "order_status":"dim_order_status_parquet",
        "geo":"dim_geo_parquet"
    }

    dims = {}
    for dim_name, cfg_key in dim_paths.items():
        path = cfg["outputs"][cfg_key]
        dims[dim_name] = spark.read.parquet(path)

    # ---------------------------
    # DIM CUSTOMER 
    # ---------------------------

    dim_customer = dims["customer"].join(
        dims["date"].select(F.col("date").alias("tmp_date"),"date_sk").withColumnRenamed("date_sk","date_sk_registration"), 
        dims["customer"]["registration_date"] == F.col("tmp_date"), how="left").drop("tmp_date")
    
    dim_customer = dim_customer.join(
        dims["date"].select(F.col("date").alias("tmp_date"),"date_sk").withColumnRenamed("date_sk","date_sk_activation"), 
        dim_customer["activation_date"] == F.col("tmp_date"), how="left").drop("tmp_date")
    dim_customer = dim_customer.join(
        dims["date"].select(F.col("date").alias("tmp_date"),"date_sk").withColumnRenamed("date_sk","date_sk_deactivation"), 
        dim_customer["deactivation_date"] == F.col("tmp_date"), how="left").drop("tmp_date")

    dim_customer = dim_customer.join(dims["geo"].select("country","region","city","geo_sk"), 
        on=["country","region","city"], how="left")

    # ---------------------------
    # DIM PRODUCT 
    # ---------------------------

    dim_product = dims["product"].join(dims["category"].select("category_code","category_sk"), 
        (dims["product"]["category_code_es"] == F.col("category_code")) | (dims["product"]["category_code_uk"] == F.col("category_code")), how="left")
    
    # ---------------------------
    # FACT: items + orders
    # ---------------------------

    orders = orders.drop(orders["source_system"])
    df = items.join(orders, items["order_id"] == orders["global_order_id"], how="inner")

    df = df.join(
        dims["date"].select(F.col("date").alias("tmp_date"),"date_sk").withColumnRenamed("date_sk","date_sk_order"),
        df["order_date"] == F.col("tmp_date"), how="left").drop("tmp_date")
    df = df.join(
        dims["date"].select(F.col("date").alias("tmp_date"),"date_sk").withColumnRenamed("date_sk","date_sk_ship"),
        df["ship_date"] == F.col("tmp_date"), how="left").drop("tmp_date")

    df = df.join(dim_customer.select("global_customer_id","customer_sk"),on="global_customer_id", how="left")
    df = df.join(dim_product.select("product_code","product_sk"),on="product_code", how="left")
    df = df.join(dims["shipping"].select("shipping_method_code","shipping_method_sk"), df["shipping_method_id"] == dims["shipping"]["shipping_method_code"], how="left")

    # ---------------------------
    # DIM PAYMENT_TYPE
    # ---------------------------
    ref_df = load_ref_yaml(spark, cfg["inputs"]["ref_payment_type_yaml"], "payment_type")
    df = apply_ref_mapping(df,"payment_type",ref_df,"normalize", "code","payment_type_code")
    df = df.join(dims["payment"].select("payment_type_code","payment_type_sk"),on="payment_type_code", how="left")

    # ---------------------------
    # DIM ORDER_STATUS
    # ---------------------------
    ref_df = load_ref_yaml(spark, cfg["inputs"]["ref_order_status_yaml"], "order_status")
    df = apply_ref_mapping(df,"status",ref_df,"normalize", "code","order_status_code")
    df = df.join(dims["order_status"].select("order_status_code","order_status_sk"),on="order_status_code", how="left")

    # ---------------------------
    # METRICS
    # ---------------------------

    df = df.withColumn("amount_eur", F.round(F.col("quantity") * F.col("unit_price_eur"), 2))


    fact = df.select(
        "global_order_id", "line_number", "source_system",
        "date_sk_order", "date_sk_ship",
        "customer_sk", "product_sk", 
        "shipping_method_sk", "payment_type_sk", "order_status_sk",
        "quantity", "unit_price_eur", "amount_eur"
    )

    (fact.repartition(1)
            .write.mode("overwrite")
            .parquet(out_path))

    return {
        "table": "fact_order_item",
        "rows": fact.count(),
        "cols": len(fact.columns),
        "path": out_path
    }