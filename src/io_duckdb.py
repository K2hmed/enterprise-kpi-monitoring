from __future__ import annotations
from pathlib import Path
import duckdb


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=4;")
    return con


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def run_sql_file(con: duckdb.DuckDBPyConnection, sql_path: Path, params: dict) -> None:
    sql = sql_path.read_text(encoding="utf-8")

    for k, v in params.items():
        escaped = _escape_sql_string(str(v))
        sql = sql.replace(f"${k}", f"'{escaped}'")

    con.execute(sql)