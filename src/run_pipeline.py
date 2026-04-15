from pathlib import Path
import pyodbc
import toml
from datetime import datetime
import logging
import pandas as pd

PIPELINE_STEPS = [
    # 1. cleaning
    "sql/cleaning/cleaning_checks.sql",

    # 2. marts
    "sql/marts/Dimension tables (date, customer, product).sql",
    "sql/marts/Fact table (sales).sql",

    # 3. validation
    "sql/validation/check_kpi.sql",
]


def load_config():
    config_path = Path(__file__).resolve().parents[1] / "config" / "config.toml"
    return toml.load(config_path)


def get_connection(cfg):
    db = cfg["database"]

    conn_str = (
        f"DRIVER={{{db['driver']}}};"
        f"SERVER={db['server']};"
        f"DATABASE={db['database']};"
        f"Trusted_Connection={db['trusted_connection']};"
        f"TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)


def run_sql_file(cursor, sql_file_path):
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    statements = []
    current_block = []

    for line in sql_script.splitlines():
        if line.strip().upper() == "GO":
            if current_block:
                statements.append("\n".join(current_block))
                current_block = []
        else:
            current_block.append(line)

    if current_block:
        statements.append("\n".join(current_block))

    for stmt in statements:
        if stmt.strip():
            cursor.execute(stmt)

def setup_logging(project_root):
    log_dir = project_root / "output" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"pipeline_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler()
        ],
        force=True,
    )
    return log_path


def export_views_to_csv(conn, project_root):
    export_dir = project_root / "output" / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    export_queries = {
        "vw_fact_sales.csv": "SELECT * FROM dbo.vw_fact_sales;",
        "vw_dim_product.csv": "SELECT * FROM dbo.vw_dim_product;",
        "vw_dim_territory.csv": "SELECT * FROM dbo.vw_dim_territory;",
    }

    for filename, query in export_queries.items():
        df = pd.read_sql(query, conn)
        output_path = export_dir / filename
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logging.info("Exported %s | rows=%s", filename, len(df))

def main():
    project_root = Path(__file__).resolve().parents[1]
    log_path = setup_logging(project_root)

    cfg = load_config()
    conn = get_connection(cfg)
    cursor = conn.cursor()

    try:
        logging.info("Pipeline started")

        for step in PIPELINE_STEPS:
            sql_path = project_root / step
            print(f"Running: {sql_path}")
            logging.info("Running step: %s", sql_path)

            run_sql_file(cursor, sql_path)
            conn.commit()

            logging.info("Committed step: %s", sql_path.name)

        # validation 실행
        validation_query = """
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN Revenue IS NULL THEN 1 ELSE 0 END) AS null_revenue_rows,
            COUNT(DISTINCT SalesOrderID) AS distinct_orders,
            COUNT(DISTINCT SalesOrderDetailID) AS distinct_order_lines,
            COUNT(*) - COUNT(DISTINCT SalesOrderDetailID) AS duplicate_line_rows,
            SUM(Revenue) AS total_revenue,
            COUNT(DISTINCT CustomerID) AS customer_count,
            SUM(Revenue) / COUNT(DISTINCT SalesOrderID) AS AOV
        FROM dbo.vw_fact_sales;
        """

        validation_df = pd.read_sql(validation_query, conn)

        print("\nValidation result:")
        print(validation_df)

        logging.info("Validation result:\n%s", validation_df.to_string(index=False))

        if validation_df["total_rows"][0] == 0:
            raise Exception("❌ No data in fact table")

        if validation_df["null_revenue_rows"][0] > 0:
            raise Exception("❌ Revenue contains NULLs")

        if validation_df["duplicate_line_rows"][0] > 0:
            raise Exception("❌ Duplicate order lines detected")

        print("✅ Validation passed")
        logging.info("Validation passed")

        # CSV export
        export_views_to_csv(conn, project_root)
        logging.info("CSV export completed")

        print("Pipeline completed successfully.")
        print(f"Log saved to: {log_path}")
        logging.info("Pipeline completed successfully")

    except Exception as e:
        conn.rollback()
        print(f"Pipeline failed: {e}")
        logging.exception("Pipeline failed")

    finally:
        cursor.close()
        conn.close()
        logging.info("Connection closed")


if __name__ == "__main__":
    main()

