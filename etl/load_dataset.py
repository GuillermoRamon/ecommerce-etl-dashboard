from pathlib import Path
from etl.utils.paths import RAW_ES, RAW_UK, INTERMEDIATE_UNIFIED
import importlib

def create_parquet(table, run_fn):

    in_es = RAW_ES / f"{table}_spain.csv"
    in_uk = RAW_UK / f"{table}_uk.csv"
    out_path = INTERMEDIATE_UNIFIED / f"{table}.parquet"

    summary = run_fn(in_es, in_uk, out_path)

    print(f"Path: {summary['path']}")
    print(f"Rows: {summary['rows']}, Columns: {summary['cols']}")


def main():
    tables = {
        "categories": "etl.steps.unify_categories",
        "products": "etl.steps.unify_products",
        "orders": "etl.steps.unify_orders",
        "customers": "etl.steps.unify_customers",
        "order_items": "etl.steps.unify_order_items",
    }
    
    summaries = []
    for table, module_path in tables.items():
        mod = importlib.import_module(module_path)
        run_fn = getattr(mod, "run")
        summaries.append(create_parquet(table, run_fn))
    return summaries

if __name__ == "__main__":
    main()