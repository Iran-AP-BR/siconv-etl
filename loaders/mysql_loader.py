# -*- coding: utf-8 -*-

from etl import rows_print, feedback, LoaderClass
import gc
from datetime import datetime, timezone, timedelta
from loaders.mysql_loader_exceptions import *


class MySQLLoader(LoaderClass):
    def __init__(self, database, logger )-> None:
        super().__init__()
        self.logger = logger
        self.database = database 
        self.engine = None

    def load(self):
        try:
            
            self.__connect_database__()
            self.__load__()
                
        except Exception as e:
        
            raise Exception(f'MySQLLoader: {str(e)}')
        
    def __load__(self):
        self.logger.info('[Loading to Database]')
        
        self.__load_table__("estados")
        self.__load_table__("municipios")
        self.__load_table__("proponentes")
        self.__load_table__("emendas")
        self.__load_table__("emendas_convenios")
        self.__load_table__("licitacoes")
        self.__load_table__("calendario")
        self.__load_table__("convenios")
        self.__load_table__("fornecedores")
        self.__load_table__("movimento")
        self.__load_table__("atributos")
        self.__load_table__("data_atual")

        self.logger.info('Database loading: Processo finalizado com sucesso!')

    def __load_table__(self, table_name):
        feedback(self.logger, label=f'-> {table_name}', value='updating...')

        try:
            
            table = self.read_data(table_name=table_name)
            self.database.write(data_frame=table, table_name=table_name)

        except Exception as e:
            
            raise Exception(f'MySQLLoader: {str(e)}')
        
        feedback(self.logger, label=f'-> {table_name}', value=f'{rows_print(table)}')
        
        del table
        gc.collect()

    def __connect_database__(self):
        try:
            
            feedback(self.logger, label='-> database', value='connecting...')
            self.database.connect_database()
            feedback(self.logger, label='-> database', value='Success!')
        
        except Exception as e:
        
            feedback(self.logger, label='-> database', value='DATABASE NOT CONNECTED!')
            raise e

    def __check_database__(self, force_update=False):

        current_database_date = self.database.check_database()

        current_database_date_str = 'sem data!' if current_database_date is None \
                                                else current_database_date.strftime("%Y-%m-%d")

        feedback(self.logger, label='-> (database) última data', value=current_database_date_str)

        current_files_date = self.file_tools.get_files_date(with_exception=True)

        today = datetime.now(timezone(timedelta(hours=-3))).date()
        if not force_update and current_database_date:
            if  current_database_date >= today:
                raise DBUpToDateException()

            if current_database_date == current_files_date:
                raise DBUnchangedException()
    