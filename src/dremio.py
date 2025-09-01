import subprocess
import time
import requests
from utils import *

def launch_docker_compose():
    try:
        subprocess.Popen(["docker", "compose", "up", "-d"])
        logger.info("Docker Compose launched in background")
    except Exception as e:
        logger.error(f"Error when launching Docker Compose: {e}")
        raise

def wait_service(url: str, timeout_sec: int) -> bool:
    start_time = time.time()
    logger.info(f"Verify the service at {url}")
    
    while time.time() - start_time < timeout_sec:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logger.info("Service available !")
                return True
        except requests.RequestException:
            pass
        
        logger.info("Service not available, retry in 3 seconds...")
        time.sleep(3)
    
    return False