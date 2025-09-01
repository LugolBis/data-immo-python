from load_dbt import generate_views, run_command
from dremio import launch_docker_compose, wait_service
from utils import *

def main():
    status, message = generate_views("data/DVF")
    if status == False:
        logger.error(message)
        return
    
    launch_docker_compose()
    status = wait_service("http://localhost:9047", 300)
    
    if status:
        run_command(["run", "--project-dir", "dbt_immo"])
        run_command(["test", "--project-dir", "dbt_immo"])
    else:
        logger.error("Failed to launch the Docker Container")