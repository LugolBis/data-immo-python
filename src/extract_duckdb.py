from utils import *
import duckdb
import os
from pathlib import Path
from typing import Optional, Callable
from transform_duckdb import remove_duplicates_mutations

INIT_SCRIPT_PATH = "databases/init.sql"
FILE_PATTERN = "mutations"

def new_connection(db_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    if db_path and os.path.exists(db_path):
        os.remove(db_path)
    if db_path:
        return duckdb.connect(db_path)
    else:
        return duckdb.connect()

def insert_values(conn: duckdb.DuckDBPyConnection, path: str, table_name: str) -> None:
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{path}')")

def from_folder(
    conn: duckdb.DuckDBPyConnection,
    folder_path: Path,
    transform_function: Callable[[duckdb.DuckDBPyConnection], None]
) -> None:
    with open(INIT_SCRIPT_PATH, 'r') as f:
        init_script = f.read()
    conn.execute(init_script)
    
    entries = [entry for entry in folder_path.iterdir() if entry.is_file()]
    target_folder = folder_path.parent
    target_folder.mkdir(exist_ok=True)
    
    for entry in entries:
        if entry.name.startswith(FILE_PATTERN) and entry.suffix == '.parquet':
            mutations_src = str(entry)
            classes_src = mutations_src.replace("mutations", "classes")
            
            insert_values(conn, mutations_src, "mutations")
            insert_values(conn, classes_src, "classes")
    
    transform_function(conn)
    
    mutations_dest = str(target_folder / "mutations.parquet")
    classes_dest = mutations_dest.replace("mutations", "classes")
    
    export_to_parquet(conn, mutations_dest, "mutations")
    export_to_parquet(conn, classes_dest, "classes")

def export_to_parquet(conn: duckdb.DuckDBPyConnection, file_path: str, table_name: str) -> None:
    conn.execute(f"COPY {table_name} TO '{file_path}' (FORMAT PARQUET)")

def main(folder_path: str, db_path: Optional[str] = None) -> str:
    try:
        conn = new_connection(db_path)
        path = Path(folder_path)
        from_folder(conn, path, remove_duplicates_mutations)
        return "Successfully transformed the data with DuckDB!"
    except Exception as e:
        return f"Error: {str(e)}"