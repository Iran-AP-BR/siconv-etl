# -*- coding: utf-8 -*-

from etl import getLogger, rows_print, feedback, LoaderClass
from pathlib import Path


class JSONLoader(LoaderClass):
    def __init__(self, path) -> None:
        super().__init__()
        self.logger = getLogger()
        self.path = path

    def load(self):
        self.logger.info('[Loading to JSON]')
        
        Path(self.path).mkdir(parents=True, exist_ok=True)

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

        self.logger.info('[JSON load complete]')

    def __load_table__(self, table_name):
        feedback(self.logger, label=f'-> {table_name}', value='updating...')

        table = self.read_data(table_name=table_name)
        table.to_json(Path(self.path).joinpath(f'{table_name}.json'), 
                      orient='records', force_ascii=False)
                      
        feedback(self.logger, label=f'-> {table_name}', value=f'{rows_print(table)}')
