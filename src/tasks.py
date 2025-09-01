import extract_api_dvf
import extract_duckdb
import load
from utils import *

def task1() -> bool:
    try:
        extract_api_dvf.main("data/FranceGeoJSON")
        print("Successfully extract the Data from the API DVF+ !")
        return True
    except Exception as error:
        logger.error(error)
        print("Failed the Task1")
        return False

def task2() -> bool:
    try:
        extract_duckdb.main("data/DVF/extracted", "db_temp.duckdb")
        return True
    except Exception as error:
        logger.error(error)
        print("Failed the Task2")
        return False

def task3() -> bool:
    try:
        load.main()
        return True
    except Exception as error:
        logger.error(error)
        print("Failed the Task3")
        return False