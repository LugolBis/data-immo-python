import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

@dataclass
class SharedMutationProps:
    vefa: bool
    typologie: str
    datemut: str
    nature: str

class Adresse:
    def __init__(
        self, btq: Optional[str], voie: Optional[str], novoie: Optional[str],
        codvoie: Optional[str], commune: Optional[str], typvoie: Optional[str],
        codepostal: Optional[str]
    ):
        self.btq = btq
        self.voie = voie
        self.novoie = novoie
        self.codvoie = codvoie
        self.commune = commune
        self.typvoie = typvoie
        self.codepostal = codepostal

    @classmethod
    def extract(cls, value: Any) -> 'Adresse':
        if not isinstance(value, list) or len(value) == 0:
            raise ValueError("Inconsistent value: Expected a non-empty list")
        adresse = value[0]
        if not isinstance(adresse, dict):
            raise ValueError("Inconsistent value: Expected a dictionary")
        
        return cls(
            btq=cls.unwrap_value(adresse.get('btq')),
            voie=cls.unwrap_value(adresse.get('voie')),
            novoie=cls.unwrap_value(adresse.get('novoie')),
            codvoie=cls.unwrap_value(adresse.get('codvoie')),
            commune=cls.unwrap_value(adresse.get('commune')),
            typvoie=cls.unwrap_value(adresse.get('typvoie')),
            codepostal=cls.unwrap_value(adresse.get('codepostal'))
        )
    
    @staticmethod
    def unwrap_value(value: Any) -> Optional[str]:
        if isinstance(value, str):
            return value
        return None

class Mutation:
    def __init__(
        self, idg: int, idpar: str, idmutation: int, shared_props: SharedMutationProps,
        adresse: Adresse, valeur_fonciere: float, vendu: bool
    ):
        self.idg = idg
        self.idpar = idpar
        self.idmutation = idmutation
        self.shared_props = shared_props
        self.adresse = adresse
        self.valeur_fonciere = valeur_fonciere
        self.vendu = vendu

    @classmethod
    def extract(cls, map: Dict[str, Any], shared_props: SharedMutationProps, valeurfonc: float,
                idmutation: int, id: int) -> 'Mutation':
        idpar = map.get('idpar')
        if not isinstance(idpar, str):
            raise ValueError("Missing or invalid 'idpar'")
        
        vendu = map.get('parcvendue')
        if not isinstance(vendu, bool):
            raise ValueError("Missing or invalid 'parcvendue'")
        
        adresses = map.get('adresses')
        if adresses is None:
            raise ValueError("Missing 'adresses'")
        
        adresse = Adresse.extract(adresses)
        
        return cls(
            idg=id,
            idpar=idpar,
            idmutation=idmutation,
            shared_props=shared_props,
            adresse=adresse,
            valeur_fonciere=valeurfonc,
            vendu=vendu
        )

class Classes:
    def __init__(self, idg: int, libelle: str, surface: float):
        self.idg = idg
        self.libelle = libelle
        self.surface = surface

    @classmethod
    def extract(cls, values: List[Any], id: int) -> List['Classes']:
        results = []
        for value in values:
            if not isinstance(value, dict):
                continue
            surface = value.get('surface')
            libelle = value.get('libregroupement')
            if (isinstance(surface, (int, float)) and surface >= 1.0 and
                isinstance(libelle, str)):
                results.append(cls(idg=id, libelle=libelle, surface=surface))
        return results

def parse_date(date_str: str) -> int:
    base_date = datetime(1970, 1, 1)
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return 0
    return (date_obj - base_date).days

def write_classes_to_parquet(data: List[Classes], path: str):
    idg_vec = []
    libelle_vec = []
    surface_vec = []
    
    for class_obj in data:
        idg_vec.append(class_obj.idg)
        libelle_vec.append(class_obj.libelle)
        surface_vec.append(class_obj.surface)
    
    schema = pa.schema([
        pa.field('idg', pa.uint64(), False),
        pa.field('libelle', pa.string(), False),
        pa.field('surface', pa.float64(), False)
    ])
    
    table = pa.Table.from_arrays([
        pa.array(idg_vec, type=pa.uint64()),
        pa.array(libelle_vec, type=pa.string()),
        pa.array(surface_vec, type=pa.float64())
    ], schema=schema)
    
    pq.write_table(table, path, compression='SNAPPY')

def write_mutations_to_parquet(data: List[Mutation], path: str):
    idg_vec = []
    idpar_vec = []
    idmutation_vec = []
    vefa_vec = []
    typologie_vec = []
    datemut_vec = []
    nature_vec = []
    btq_vec = []
    voie_vec = []
    novoie_vec = []
    codvoie_vec = []
    commune_vec = []
    typvoie_vec = []
    codepostal_vec = []
    valeur_fonciere_vec = []
    vendu_vec = []
    
    for mutation in data:
        idg_vec.append(mutation.idg)
        idpar_vec.append(mutation.idpar)
        idmutation_vec.append(mutation.idmutation)
        vefa_vec.append(mutation.shared_props.vefa)
        typologie_vec.append(mutation.shared_props.typologie)
        datemut_vec.append(parse_date(mutation.shared_props.datemut))
        nature_vec.append(mutation.shared_props.nature)
        btq_vec.append(mutation.adresse.btq)
        voie_vec.append(mutation.adresse.voie)
        novoie_vec.append(mutation.adresse.novoie)
        codvoie_vec.append(mutation.adresse.codvoie)
        commune_vec.append(mutation.adresse.commune)
        typvoie_vec.append(mutation.adresse.typvoie)
        codepostal_vec.append(mutation.adresse.codepostal)
        valeur_fonciere_vec.append(mutation.valeur_fonciere)
        vendu_vec.append(mutation.vendu)
    
    schema = pa.schema([
        pa.field('idg', pa.uint64(), False),
        pa.field('idpar', pa.string(), False),
        pa.field('idmutation', pa.uint64(), False),
        pa.field('vefa', pa.bool_(), False),
        pa.field('typologie', pa.string(), True),
        pa.field('datemut', pa.date32(), False),
        pa.field('nature', pa.string(), True),
        pa.field('btq', pa.string(), True),
        pa.field('voie', pa.string(), True),
        pa.field('novoie', pa.string(), True),
        pa.field('codvoie', pa.string(), True),
        pa.field('commune', pa.string(), True),
        pa.field('typvoie', pa.string(), True),
        pa.field('codepostal', pa.string(), True),
        pa.field('valeur_fonciere', pa.float64(), False),
        pa.field('vendu', pa.bool_(), True)
    ])
    
    table = pa.Table.from_arrays([
        pa.array(idg_vec, type=pa.uint64()),
        pa.array(idpar_vec, type=pa.string()),
        pa.array(idmutation_vec, type=pa.uint64()),
        pa.array(vefa_vec, type=pa.bool_()),
        pa.array(typologie_vec, type=pa.string()),
        pa.array(datemut_vec, type=pa.date32()),
        pa.array(nature_vec, type=pa.string()),
        pa.array(btq_vec, type=pa.string()),
        pa.array(voie_vec, type=pa.string()),
        pa.array(novoie_vec, type=pa.string()),
        pa.array(codvoie_vec, type=pa.string()),
        pa.array(commune_vec, type=pa.string()),
        pa.array(typvoie_vec, type=pa.string()),
        pa.array(codepostal_vec, type=pa.string()),
        pa.array(valeur_fonciere_vec, type=pa.float64()),
        pa.array(vendu_vec, type=pa.bool_())
    ], schema=schema)
    
    pq.write_table(table, path, compression='SNAPPY')