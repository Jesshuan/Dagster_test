
from dagster import Config
    
# ASSET CONFIG & FACTORY


class AssetConfig(Config):
    model_type: str
    params: dict
    sources: list

class OpPreprocInterpConfig(Config):
    interpolation_type : str

class OpPreprocTimeConfig(Config):
    time_preprocessing_type : str

class OpMainProcessConfig(Config):
    model_type : str
    params : dict
