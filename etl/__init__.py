# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime, timezone, timedelta
from .extraction import Extraction
from .transformation import Transformation
from .data_files_tools import FileTools
from .data_files_exceptions import *
from .utils import *
from .loader import LoaderClass
import nltk
from pathlib import Path
from etl.config import Config
import logging


def getLogger():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    return logger

class ETL(object):
    def __init__(self, loader=None) -> None:
        assert loader is None or issubclass(loader.__class__, LoaderClass), \
               "loader argument must a subclass of 'LoaderClass' abstract class"

        self.config = Config()
        self.logger = getLogger()
        self.loader = loader
        self.file_tools = FileTools()

        nltk_path = self.config.NLTK_DATA
        if nltk_path and Path(nltk_path).exists():
            nltk.data.path.append(nltk_path)

    def pipeline(self, force_download=False, force_transformations=False):
        try:
            current_date = self.check_update(force_update=force_transformations or force_download)

            self.extractor = Extraction(logger=self.logger, 
                                        current_date=current_date)
            self.extractor.extract(force_download=force_download)

            self.transformer = Transformation(logger=self.logger, 
                                              current_date=current_date)
            self.transformer.transform()

        except FILESUpToDateException:
            self.logger.info('Data Files: Dados já estão atualizados.')
        except FILESUnchangedException:
            self.logger.info('Data Files: Dados inalterados na origem.')
        except Exception as e:
            raise Exception(f'Data Files: {str(e)}')


        if self.loader is not None:
            self.loader.load()

        self.logger.info('Processo finalizado com sucesso!')


    def check_update(self, force_update=False):

        def getCurrentDate():
            current_date = pd.read_csv(self.config.CURRENT_DATE_URI,
                                       compression=self.config.CURRENT_DATE_URI_COMPRESSION,
                                       dtype=str).head(1)
            return datetime_validation(current_date['data_carga'][0])

        self.logger.info('[Getting current date]')

        feedback(self.logger, label='-> data atual', value='connecting...')

        current_date = getCurrentDate()

        feedback(self.logger, label='-> data atual', value=current_date.strftime("%Y-%m-%d"))

        last_date = self.file_tools.get_files_date()
        last_date_str = last_date.strftime("%Y-%m-%d") if last_date else 'Inexistente'
        feedback(self.logger, label='-> última data', value=last_date_str)


        today = datetime.now(timezone(timedelta(hours=-3))).date()
        if not force_update and last_date:
            if  last_date >= today:
                raise FILESUpToDateException()

            if last_date == current_date:
                raise FILESUnchangedException()

        return current_date
