from pathlib import Path
import pyodbc
import toml


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
    )
    return pyodbc.connect(conn_str)


def run_sql_file(cursor, sql_file_path):
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    # GO 분리 처리
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
    sql_dir = project_root / "sql"

    sql_files = [
        sql_dir / "01_extract_sales.sql",
        sql_dir / "02_build_marts.sql",
        sql_dir / "03_kpi_summary.sql",
    ]

    cfg = load_config()

    conn = get_connection(cfg)
    cursor = conn.cursor()

    try:
        for sql_file in sql_files:
            print(f"Running: {sql_file.name}")
            run_sql_file(cursor, sql_file)
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