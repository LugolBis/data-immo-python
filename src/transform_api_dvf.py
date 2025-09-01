import json
from typing import List, Dict, Any, Optional, Tuple
from tables import SharedMutationProps, Mutation, Classes
from utils import *

def map_parcelles(
    parcelles: List[Dict[str, Any]],
    shared_props: SharedMutationProps,
    valeurfonc: float,
    idmutation: int,
    idg: int,
) -> Tuple[int, List[Mutation], List[Classes]]:
    mutations = []
    classes = []

    for parcelle in parcelles:
        if 'dcnt' not in parcelle:
            raise ValueError("Failed to get the value of the key 'dcnt'")
        dcnt = parcelle['dcnt']
        if not isinstance(dcnt, list):
            raise ValueError("Inconsistent value: Expected a list")
        
        idg += 1
        
        classes_rows = Classes.extract(dcnt, idg)
        mutation_row = Mutation.extract(parcelle, shared_props, valeurfonc, idmutation, idg)
        
        classes.extend(classes_rows)
        mutations.append(mutation_row)
    
    return idg, mutations, classes

def map_dispositions(
    dispositions: List[Any],
    shared_props: SharedMutationProps,
    idg: int,
) -> Tuple[int, List[Mutation], List[Classes]]:
    mutations = []
    classes = []

    valid_dispositions = []
    for disposition in dispositions:
        if isinstance(disposition, dict):
            valid_dispositions.append(disposition)
    
    for disposition in valid_dispositions:
        if 'parcelles' not in disposition:
            raise ValueError("Failed to get the value of the key 'parcelles'")
        parcelles = disposition['parcelles']
        if not isinstance(parcelles, list):
            raise ValueError("Inconsistent value: Expected a list")
        
        valid_parcelles = []
        for parcelle in parcelles:
            if isinstance(parcelle, dict):
                valid_parcelles.append(parcelle)
        
        if 'valeurfonc' not in disposition:
            raise ValueError("Failed to get the value of the key 'valeurfonc'")
        valeurfonc = disposition['valeurfonc']
        if not isinstance(valeurfonc, (int, float)):
            raise ValueError("Inconsistent value: Expected a number")
        
        if 'idmutation' not in disposition:
            raise ValueError("Failed to get the value of the key 'idmutation'")
        idmutation = disposition['idmutation']
        if not isinstance(idmutation, int):
            raise ValueError("Inconsistent value: Expected an integer")
        
        try:
            new_idg, muts, clas = map_parcelles(
                valid_parcelles,
                shared_props,
                valeurfonc,
                idmutation,
                idg
            )

            idg = new_idg
            mutations.extend(muts)
            classes.extend(clas)
        except Exception as error:
            logger.error(f"{error}")

    return idg, mutations, classes

def map_properties(
    properties: Dict[str, Any],
    idg: int,
) -> Tuple[int, List[Mutation], List[Classes]]:
    if 'vefa' not in properties:
        raise ValueError("Failed to get the value of the key 'vefa'")
    vefa = properties['vefa']
    if not isinstance(vefa, bool):
        raise ValueError("Inconsistent value: Expected a boolean")
    
    if 'datemut' not in properties:
        raise ValueError("Failed to get the value of the key 'datemut'")
    datemut = properties['datemut']
    if not isinstance(datemut, str):
        raise ValueError("Inconsistent value: Expected a string")
    
    if 'typologie' not in properties:
        raise ValueError("Failed to get the value of the key 'typologie'")
    typologie_obj = properties['typologie']
    if not isinstance(typologie_obj, dict):
        raise ValueError("Inconsistent value: Expected a dictionary")
    if 'libelle' not in typologie_obj:
        raise ValueError("Failed to get the value of the key 'libelle'")
    typologie = typologie_obj['libelle']
    if not isinstance(typologie, str):
        raise ValueError("Inconsistent value: Expected a string")
    
    if 'nature_mutation' not in properties:
        raise ValueError("Failed to get the value of the key 'nature_mutation'")
    nature_obj = properties['nature_mutation']
    if not isinstance(nature_obj, dict):
        raise ValueError("Inconsistent value: Expected a dictionary")
    if 'libelle' not in nature_obj:
        raise ValueError("Failed to get the value of the key 'libelle'")
    nature = nature_obj['libelle']
    if not isinstance(nature, str):
        raise ValueError("Inconsistent value: Expected a string")
    
    shared_props = SharedMutationProps(vefa, typologie, datemut, nature)
    
    if 'dispositions' not in properties:
        raise ValueError("Failed to get the value of the key 'dispositions'")
    dispositions = properties['dispositions']
    if not isinstance(dispositions, list):
        raise ValueError("Inconsistent value: Expected a list")
    
    return map_dispositions(
        dispositions,
        shared_props,
        idg
    )

def transform_api_data(
    data: str,
    idg: int,
) -> Tuple[int, List[Mutation], List[Classes]]:
    mutations = []
    classes = []

    try:
        value = json.loads(data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to convert the data content to JSON: {e}")
    
    if not isinstance(value, dict):
        raise ValueError("Inconsistent value: Expected a dictionary")
    
    if 'features' not in value:
        raise ValueError("Failed to get the value of the key 'features'")
    features = value['features']
    if not isinstance(features, list):
        raise ValueError("Inconsistent value: Expected a list")
    
    for feature in features:
        if not isinstance(feature, dict):
            continue
        
        if 'properties' not in feature:
            raise ValueError("Failed to get the value of the key 'properties'")
        properties = feature['properties']
        if not isinstance(properties, dict):
            raise ValueError("Inconsistent value: Expected a dictionary")
        
        try:
            new_idg, muts, clas = map_properties(properties, idg)

            idg = new_idg
            mutations.extend(muts)
            classes.extend(clas)
        except Exception as error:
            logger.error(f"{error}")
    
    return idg, mutations, classes