from pathlib import Path
from etl.utils.paths import RAW_ES, RAW_UK, INTERMEDIATE_UNIFIED
from etl.steps.unify_categories import run as run_unify_categories
from etl.steps.unify_products import run as run_unify_products
from etl.steps.unify_orders import run as run_unify_orders
from etl.steps.unify_customers import run as run_unify_customers
from etl.steps.unify_order_items import run as run_unify_order_items


def main():

    print("Unifying categories...")
    summary_categories = run_unify_categories(
        RAW_ES / "categories_spain.csv",
        RAW_UK / "categories_uk.csv",
        INTERMEDIATE_UNIFIED / "categories.parquet"
        )

    print(f"Path: {summary_categories['path']}")
    print(f"Rows: {summary_categories['rows']}, Columns: {summary_categories['cols']}")


    print("Unifying products...")
    summary_products = run_unify_products(
        RAW_ES / "products_spain.csv",
        RAW_UK / "products_uk.csv",
        INTERMEDIATE_UNIFIED / "products.parquet"
    )

    print(f"Path: {summary_products['path']}")
    print(f"Rows: {summary_products['rows']}, Columns: {summary_products['cols']}")


    print("Unifying orders...")
    summary_orders = run_unify_orders(
        RAW_ES / "orders_spain.csv",
        RAW_UK / "orders_uk.csv",
        INTERMEDIATE_UNIFIED / "orders.parquet"
    )

    print(f"Path: {summary_orders['path']}")
    print(f"Rows: {summary_orders['rows']}, Columns: {summary_orders['cols']}")
    

    print("Unifying customers...")
    summary_customers = run_unify_customers(
        RAW_ES / "customers_spain.csv",
        RAW_UK / "customers_uk.csv",
        INTERMEDIATE_UNIFIED / "customers.parquet"
    )

    print(f"Path: {summary_customers['path']}")
    print(f"Rows: {summary_customers['rows']}, Columns: {summary_customers['cols']}")


    print("Unifying order items...")
    summary_order_items = run_unify_order_items(
        RAW_ES / "order_items_spain.csv",
        RAW_UK / "order_items_uk.csv",
        INTERMEDIATE_UNIFIED / "order_items.parquet"
    )

    print(f"Path: {summary_order_items['path']}")
    print(f"Rows: {summary_order_items['rows']}, Columns: {summary_order_items['cols']}")

if __name__ == "__main__":
    main()