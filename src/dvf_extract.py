from utils import *
from typing import Any
import requests
from requests import Response
import json
import os
from datetime import date
import time

load_dotenv()

API_KEY_DVFPLUS = os.environ.get("API_KEY_DVFPLUS")

HEADERS = {
    'Content-Type': 'application/json',
    'accept': 'application/json'
}

# Default filters
FILTERS = {
    "valeurfonc[lte]": "100000000000000000",
    "datemut[lt]": date.today().strftime("%Y-%m-%d"),
    "buffer": "0"
}

def api_get(endpoint: str, headers: dict = {}, body: dict = {}) -> Result[dict, str]:
    url = f"https://api.sogefi-sig.com/{API_KEY_DVFPLUS}/dvfplus/v1.0/sogefi/{endpoint}"
    if headers == {}:
        headers = HEADERS

    response: Response = requests.get(
        url=url,
        headers=HEADERS,
        data=body
    )

    if response.status_code == 200:
        try:
            return Ok(response.json())
        except Exception as error:
            return Err(f"Failed to convert to json format the following content :\n{response.reason}\nError occured : {error}")
    elif response.status_code == 402:
        return Err(f"Failed to GET '{url}' due to ecxceed request quota.")
    else:
        return Err(f"Failed to GET '{url}', status code : {response.status_code} - {response.reason}")
    
def api_post(endpoint: str, headers: dict = HEADERS, data: dict = {}, filters: dict = FILTERS) -> Result[Response, str]:
    url = f"https://api.sogefi-sig.com/{API_KEY_DVFPLUS}/dvfplus/v1.0/sogefi/{endpoint}"

    response: Response = requests.post(
        url=url,
        headers=headers,
        params=filters,
        json=data
    )

    if response.status_code == 200:
        try:
            return Ok(response)
        except Exception as error:
            return Err(f"Failed to convert to json format the following content :\n{response.reason}\nError occured : {error}")
    elif response.status_code == 402:
        return Err(f"Failed to POST '{url}' status code : 402 - Ecxceed request quota")
    else:
        return Err(f"Failed to POST '{url}', status code : {response.status_code} - {response.reason}")
    
def get_department(path: str) -> Result[dict, str]:
    try:
        with open(path, 'r') as fs:
            data = json.load(fs)
        if isinstance(data, dict):
            return Ok(data)
        else:
            return Err(f"Inconsistant data from the file {path} :\n{data}")
    except Exception as error:
        return Err(f"Error occurs when try to get the data from {path} :\n{error}")
    
def save_mutation_feature(number_feature: str, data: dict, errors_occured: int) -> Result[str, str]:
    if errors_occured > 0:
        time.sleep(60)

    match (api_post("mutation/search",data=data), errors_occured):
        case (Ok(response), default):
            with open(f'data/DVF/extracted/{number_feature}.json', 'w', encoding='utf-8') as fd:
                json.dump(response.json(), fd, ensure_ascii=False)
            return Ok(f"Successfully save the feature {number_feature} !")
        case (Err(message), 0):
            logger.error(message)
            if message.endswith(("402 - Ecxceed request quota", "500 - Internal Server Error")):
                return save_mutation_feature(number_feature, data, 1)
            else:
                return Err(message)
        case (Err(message), default):
            logger.error(message)
            return Err(message)
        case default:
            return Err("Impossible.")
    
def process_features(features: list[dict], id: int):
    for index, feature in enumerate(features):
        geometry = feature['geometry']
        data = {"geojson": geometry}

        match save_mutation_feature(f"{id}_{index}", data, 0):
            case Ok(message):
                logger.success(message)
            case Err(message):
                logger.error(message)

def main(folder_path: str):
    entries: list[str] = os.listdir(folder_path)

    for index, entry in enumerate(entries):
        match get_department(os.path.join(folder_path, entry)):
            case Ok(departement):
                features = departement['features']
                process_features(features, index)
            case Err(message):
                logger.error(message)
    
if __name__ == '__main__':
    main("data/FranceGeoJSON")