import os
import subprocess
from pathlib import Path
from typing import List
from typing import Tuple

MODEL_FOLDER = "dbt_immo/models"

def generate_view(files_path: List[str], view_name: str) -> None:
    path = Path(MODEL_FOLDER) / view_name
    path.parent.mkdir(parents=True, exist_ok=True)
    
    queries = [f'SELECT * FROM DVF."{table}"' for table in files_path]
    query = "\nUNION ALL\n".join(queries)
    
    with open(path, 'w') as file:
        file.write(query)

def generate_views(folder_path: str) -> Tuple[bool, str]:
    path: Path = Path(folder_path)
    mutations_path = []
    classes_path = []
    
    try:
        entries = [entry for entry in path.iterdir() if entry.is_file()]
    except Exception as e:
        error_msg = f"Failed to read the folder {folder_path} : {e} - {os.getcwd()}"
        print(error_msg)
        return False, error_msg
    
    for entry in entries:
        if entry.suffix == '.parquet':
            filename = entry.name
            if filename.startswith("mutations"):
                mutations_path.append(filename)
            elif filename.startswith("classes"):
                classes_path.append(filename)
    
    try:
        generate_view(mutations_path, "mutations.sql")
        generate_view(classes_path, "classes.sql")
        return True, "Successfully generate the dbt models !"
    except Exception as e:
        return False, f"Error generating views: {e}"

def run_command(args: List[str]) -> None:
    dbt_path = Path(".venv/bin/dbt")
    
    try:
        result = subprocess.run(
            [str(dbt_path)] + args,
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        print(f"Stderr: {e.stderr}")
        raise