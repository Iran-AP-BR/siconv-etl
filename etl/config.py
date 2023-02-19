# coding: utf-8

import os
from pathlib import Path

root = str(Path(__file__).parent)

class Config(object):

    CURRENT_DATE_URI = os.getenv('CURRENT_DATE_URI', 
      default='http://repositorio.dados.gov.br/seges/detru/data_carga_siconv.csv.zip')
    CURRENT_DATE_URI_COMPRESSION = os.getenv('CURRENT_DATE_URI_COMPRESSION', 'zip')
    DOWNLOAD_URI = os.getenv('DOWNLOAD_URI', 'http://repositorio.dados.gov.br/seges/detru/')    
    MUNICIPIOS_BACKUP_FOLDER = os.getenv('MUNICIPIOS_BACKUP_FOLDER', default=Path(root).joinpath('municipios'))
    NLTK_DATA = os.getenv('NLTK_DATA', default=root)
    MODEL_PATH = os.getenv('MODEL_PATH', default=Path(root).joinpath('trained_model/model.pickle'))
    DATA_FOLDER = os.getenv('DATA_FOLDER', default=Path(root).parent.joinpath('data_files/end_files'))
    STAGE_FOLDER = os.getenv('STAGE_FOLDER', default=Path(root).parent.joinpath('data_files/stage_files'))
    TIMEZONE_OFFSET = os.getenv('TIMEZONE_OFFSET', default=-3)

    COMPRESSION_METHOD = 'gzip'
    FILE_EXTENTION = '.parquet'
    CURRENT_DATE_FILENAME = 'data_atual.parquet'
    CHUNK_SIZE = 500000

