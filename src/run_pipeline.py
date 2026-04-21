from pathlib import Path
import pyodbc
import toml
from datetime import datetime, date
import logging
import pandas as pd

PIPELINE_STEPS = [
    "sql/cleaning/cleaning_checks.sql",
    "sql/marts/Dimension tables (date, customer, product).sql",
    "sql/marts/Fact table (sales).sql",
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


def write_kpi_summary(cursor, validation_df):
    row = validation_df.iloc[0]

    insert_sql = """
    INSERT INTO analytics.kpi_summary (
        run_ts,
        total_rows,
        null_revenue_rows,
        distinct_orders,
        total_revenue,
        customer_count,
        aov,
        latest_order_date,
        latest_revenue_date,
        daily_revenue,
        prev_7day_avg,
        revenue_ratio,
        revenue_drop_flag,
        zero_revenue_category_count,
        zero_revenue_category_flag
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.execute(
        insert_sql,
        datetime.now(),
        int(row["total_rows"]),
        int(row["null_revenue_rows"]),
        int(row["distinct_orders"]),
        float(row["total_revenue"]),
        int(row["customer_count"]),
        float(row["AOV"]),
        row["latest_order_date"],
        row["latest_revenue_date"],
        float(row["daily_revenue"]),
        float(row["prev_7day_avg"]) if pd.notna(row["prev_7day_avg"]) else None,
        float(row["revenue_ratio"]) if pd.notna(row["revenue_ratio"]) else None,
        int(row["revenue_drop_flag"]),
        int(row["zero_revenue_category_count"]),
        int(row["zero_revenue_category_flag"]),
    )


def run_validation(conn):
    validation_query = """
    WITH base_kpi AS (
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN Revenue IS NULL THEN 1 ELSE 0 END) AS null_revenue_rows,
            COUNT(DISTINCT SalesOrderID) AS distinct_orders,
            SUM(Revenue) AS total_revenue,
            COUNT(DISTINCT CustomerID) AS customer_count,
            SUM(Revenue) * 1.0 / NULLIF(COUNT(DISTINCT SalesOrderID), 0) AS AOV,
            MAX(CAST(OrderDate AS date)) AS latest_order_date
        FROM dbo.vw_fact_sales
    ),

    daily_sales AS (
        SELECT
            CAST(OrderDate AS date) AS order_date,
            SUM(Revenue) AS daily_revenue
        FROM dbo.vw_fact_sales
        GROUP BY CAST(OrderDate AS date)
    ),

    latest_revenue_check AS (
        SELECT TOP 1
            order_date,
            daily_revenue,
            AVG(daily_revenue) OVER (
                ORDER BY order_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS prev_7day_avg
        FROM daily_sales
        ORDER BY order_date DESC
    ),

    category_check AS (
        SELECT
            COUNT(*) AS zero_revenue_category_count
        FROM (
            SELECT
                ProductCategory
            FROM dbo.vw_fact_sales
            GROUP BY ProductCategory
            HAVING SUM(Revenue) = 0
        ) z
    )

    SELECT
        b.total_rows,
        b.null_revenue_rows,
        b.distinct_orders,
        b.total_revenue,
        b.customer_count,
        b.AOV,
        b.latest_order_date,

        r.order_date AS latest_revenue_date,
        r.daily_revenue,
        r.prev_7day_avg,

        CASE
            WHEN r.prev_7day_avg IS NULL THEN NULL
            ELSE r.daily_revenue * 1.0 / NULLIF(r.prev_7day_avg, 0)
        END AS revenue_ratio,

        CASE
            WHEN r.prev_7day_avg IS NULL THEN 0
            WHEN r.daily_revenue < r.prev_7day_avg * 0.5 THEN 1
            ELSE 0
        END AS revenue_drop_flag,

        c.zero_revenue_category_count,

        CASE
            WHEN c.zero_revenue_category_count > 0 THEN 1
            ELSE 0
        END AS zero_revenue_category_flag

    FROM base_kpi b
    CROSS JOIN latest_revenue_check r
    CROSS JOIN category_check c;
    """

    validation_df = pd.read_sql(validation_query, conn)

    logging.info("Validation result:\n%s", validation_df.to_string(index=False))
    print("\nValidation result:")
    print(validation_df)

    df = validation_df

    if df.loc[0, "total_rows"] == 0:
        raise Exception("❌ No data in fact table")

    if df.loc[0, "null_revenue_rows"] > 0:
        raise Exception("❌ Revenue contains NULLs")

    if pd.notna(df.loc[0, "revenue_drop_flag"]) and df.loc[0, "revenue_drop_flag"] == 1:
        raise Exception("❌ Revenue dropped significantly vs previous 7-day average")

    if df.loc[0, "zero_revenue_category_flag"] == 1:
        raise Exception("❌ One or more product categories have zero revenue")

    latest_date = pd.to_datetime(df.loc[0, "latest_order_date"]).date()

    if (date.today() - latest_date).days > 2:
        warning_msg = f"⚠️ Data is stale. Latest order date is {latest_date}"
        print(warning_msg)
        logging.warning(warning_msg)
    else:
        print("✅ Data freshness check passed")
        logging.info("Data freshness check passed")

    print("✅ Validation passed")
    logging.info("Validation passed")

    return validation_df


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

        validation_df = run_validation(conn)

        write_kpi_summary(cursor, validation_df)
        conn.commit()
        logging.info("KPI summary written to database")

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