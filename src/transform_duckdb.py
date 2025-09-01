import duckdb
from utils import *

with open('databases/remove_duplicates.sql', 'r') as fs:
    CLEAN_SCRIPT = fs.read()

def remove_duplicates_mutations(conn: duckdb.DuckDBPyConnection):
    conn.execute(CLEAN_SCRIPT)