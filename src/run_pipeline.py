from pathlib import Path
import pyodbc
import toml

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


def main():
    project_root = Path(__file__).resolve().parents[1]

    cfg = load_config()
    conn = get_connection(cfg)
    cursor = conn.cursor()

    try:
        for step in PIPELINE_STEPS:
            sql_path = project_root / step
            print(f"Running: {sql_path}")
            run_sql_file(cursor, sql_path)
            conn.commit()

        print("Pipeline completed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Pipeline failed: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()