# -*- coding: utf-8 -*-

from datetime import datetime, timezone, timedelta
from .extraction import Extraction
from .transformation import Transformation
from .data_files_exceptions import *
from .utils import *
from .loader import LoaderClass
from pathlib import Path
import logging
from functools import reduce
import pandas as pd
import nltk


def getLogger():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    return logger

class ETL(object):
    def __init__(self, loaders=None, config=None, file_tools=None, pd=None) -> None:
        assert (loaders is None or 
                (type(loaders) is list and 
                 reduce(lambda x, y: x and y, 
                       [issubclass(loader.__class__, LoaderClass) for loader in loaders]))), \
               "O argumento 'loaders' tem que ser 'None' ou uma lista de instaâncias de \
                subclasses da classe abstract 'LoaderClass'"

        assert pd is not None, 'Pandas não disponível.'

        self.pd = pd
        self.config = config
        self.logger = getLogger()
        self.loaders = loaders
        self.file_tools = file_tools

        nltk_path = self.config.NLTK_DATA
        if nltk_path and Path(nltk_path).exists():
            nltk.data.path.append(nltk_path)

    def pipeline(self, force_download=False, force_transformations=False):
        try:
            current_date = self.check_update(force_update=force_transformations or force_download)

            self.extractor = Extraction(logger=self.logger, 
                                        current_date=current_date,
                                        config=self.config,
                                        file_tools=self.file_tools,
                                        pd=pd)
            self.extractor.extract(force_download=force_download)

            self.transformer = Transformation(logger=self.logger, 
                                              current_date=current_date,
                                              config=self.config,
                                              file_tools=self.file_tools,
                                              pd=pd)
            self.transformer.transform()

        except FILESUpToDateException:
            self.logger.info('Data Files: Dados já estão atualizados.')
        except FILESUnchangedException:
            self.logger.info('Data Files: Dados inalterados na origem.')
        except Exception as e:
            raise Exception(f'Data Files: {str(e)}')
        
        self.__load__()

    def __load__(self):
        loaders_warnings = False
        if self.loaders is not None:
            for loader in self.loaders:
                try:
                    loader.load()
                except Exception as e:
                    loaders_warnings = True
                    self.logger.warning(f'{str(e)}')

        if loaders_warnings:
            self.logger.warning('Processo finalizado com falhas no carregamento!')
        else:
            self.logger.info('Processo finalizado com sucesso!')


    def check_update(self, force_update=False):

        def getCurrentDate():
            current_date = self.pd.read_csv(self.config.CURRENT_DATE_URI,
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
