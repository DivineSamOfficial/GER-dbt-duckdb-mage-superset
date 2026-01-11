import duckdb
import logging

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@data_loader
def create_lake_views(*args, **kwargs):
    logger.info("🚀 Starting lake ingestion (direct from Google Sheets)")
    print("🚀 Starting lake ingestion (direct from Google Sheets)")

    # Connect to DuckDB (single warehouse file)
    con = duckdb.connect("dev.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS lake;")

    sources = [
        {
            "table": "ge_data_dictionary_lake_view",
            "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRZoEkyaWo1Tb7zH8bXllESg9fCK_7cr9Le3_a8vY8i2FfhYgUXYtJPAmN6yhyvCmASLjrGfOYzjOCh/pub?gid=0&single=true&output=csv"
        },
        {
            "table": "ge_stores_lake_view",
            "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRZoEkyaWo1Tb7zH8bXllESg9fCK_7cr9Le3_a8vY8i2FfhYgUXYtJPAmN6yhyvCmASLjrGfOYzjOCh/pub?gid=1529942428&single=true&output=csv"
        },
        {
            "table": "ge_customers_lake_view",
            "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRZoEkyaWo1Tb7zH8bXllESg9fCK_7cr9Le3_a8vY8i2FfhYgUXYtJPAmN6yhyvCmASLjrGfOYzjOCh/pub?gid=1797354035&single=true&output=csv"
        },
        {
            "table": "ge_products_lake_view",
            "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRZoEkyaWo1Tb7zH8bXllESg9fCK_7cr9Le3_a8vY8i2FfhYgUXYtJPAmN6yhyvCmASLjrGfOYzjOCh/pub?gid=1904089762&single=true&output=csv"
        },
        {
            "table": "ge_exchange_rates_lake_view",
            "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRZoEkyaWo1Tb7zH8bXllESg9fCK_7cr9Le3_a8vY8i2FfhYgUXYtJPAmN6yhyvCmASLjrGfOYzjOCh/pub?gid=511444651&single=true&output=csv"
        },
        {
            "table": "ge_sales_lake_view",
            "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRZoEkyaWo1Tb7zH8bXllESg9fCK_7cr9Le3_a8vY8i2FfhYgUXYtJPAmN6yhyvCmASLjrGfOYzjOCh/pub?gid=1530903136&single=true&output=csv"
        }
    ]

    for src in sources:
        table = src["table"]
        url = src["url"]

        logger.info(f"🔄 Creating view lake.{table}")
        print(f"🔄 Creating view lake.{table}")

        con.execute(f"""
            CREATE OR REPLACE VIEW lake.{table} AS
            SELECT *
            FROM read_csv_auto('{url}', header=true);
        """)

        logger.info(f"✅ View created: lake.{table}")
        print(f"✅ View created: lake.{table}")

    con.close()
    logger.info("🎉 Lake ingestion completed successfully")
    print("🎉 Lake ingestion completed successfully")

    return {"status": "ok"}
