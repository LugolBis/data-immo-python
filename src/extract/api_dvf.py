from utils import *
from typing import Any, AnyStr
import requests
from requests import Response
import json
import os
from datetime import date
import time
import re

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

TARGET_FOLDER = "data/DVF/extracted"

REGEX_ERROR = re.compile(r"""403\s*:\s*\{"message":"Surface\s+(.*?)\s+du\s+GeoJSON\s+trop\s+grande"\}""")
    
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
    
def process_feature(feature_id: str, idg: int, data: dict) -> Result[int, str]:
    failed_retry = False
    buffer: list[dict] = [data]

    mutations: list[Mutation] = []
    classes: list[Classes] = []

    while len(buffer) > 0:
        api_response: Result[Response, str] = api_post(
            endpoint="mutation/search",
            data=buffer[0]
        )

        match (api_response, failed_retry):
            case (Ok(response), default):
                content = response.text()
                mut_data, cls_data = transform_api_data(content, idg)

                mutations.extend(mut_data)
                classes.extend(cls_data)
                buffer.pop(0)
                idg += 1
                break
            case (Err(message), True):
                logger.error(message)
                buffer.pop(0)
                failed_retry = False
                break
            case (Err(message), False):
                logger.error(message)
                if "402" in message or "501" in message:
                    time.sleep(60)
                    failed_retry = True
                elif REGEX_ERROR.match(message):
                    geometry = buffer[0].get("geojson")
                    if geometry:
                        match split_geometry(geometry):
                            case Ok((geometry1, geometry2)):
                                buffer.pop(0)
                                buffer.append({'geojson': geometry1})
                                buffer.append({'geojson': geometry2})
                                break
                            case Err(message):
                                logger.error(message)
                                break
                            case default:
                                logger.error("Impossible")
                                break
                    else:
                        logger.error("Failed to get the key 'geojson'")
                    break
                else:
                    buffer.pop(0)
                    break

    mutations_path = os.path.join(TARGET_FOLDER, f"mutations_{feature_id}.parquet")
    classes_path = os.path.join(TARGET_FOLDER, f"classes_{feature_id}.parquet")

    if len(mutations) > 0:
        # TODO! write_parque_data()
        return Ok(idg)
    else:
        return Err(f"Incomplete values {feature_id}")

def process_features(features: list[dict], idg: int, dpt: int) -> Result[int, str]:
    for index, feature in enumerate(features):
        geometry = feature['geometry']
        data = {"geojson": geometry}
        feature_id = f"{dpt}{index}"

        result: Result[int, str] = process_feature(feature_id, idg, data)

        match result:
            case Ok(nb):
                idg = nb
                break
            case Err(message):
                logger.error(message)

    return Ok(idg)

def set_up(folder_path: str) -> Result[os._ScandirIterator[AnyStr@scandir], str]:
    if os.path.exists(TARGET_FOLDER) == False:
        try:
            os.makedirs(TARGET_FOLDER, exist_ok=True)
        except Exception as error:
            return Err(f"{error}")
    
    entries = os.scandir(TARGET_FOLDER)
    return Ok(entries)

def main(folder_path: str) -> Result[str, str]:
    match set_up(folder_path):
        case Ok(entries):
            dpt = 1

            for entry in entries:
                if entry.is_file():
                    match get_department(entry.name):
                        case Ok(map):
                            features = map['features']
                            if process_features(features).is_err():
                                logger.error(f"Failed to process the features of the departement {dpt}")
                            
                            dpt+=1
                            break
                        case Err(message):
                            logger.error(message)
                            break
                        case default:
                            break

            break
        case Err(message):
            logger.error(message)
            return Err(message)

            break
        case default:
            break
    
    return Ok("Successfully extract the data !")
    
if __name__ == '__main__':
    main("data/FranceGeoJSON")