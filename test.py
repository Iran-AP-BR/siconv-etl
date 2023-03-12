# coding: utf-8

from loaders.json_loader import JSONLoader
from loaders.mysql_loader import MySQLLoader
from etl import ETL, getLogger
from pathlib import Path
from etl.config import Config
from etl.data_files_tools import FileTools
from sqlalchemy_utils import database_exists
import sqlalchemy as sa
from os import getenv
import pandas as pd


SQLALCHEMY_DATABASE_URI = getenv('SQLALCHEMY_DATABASE_URI', 
                                 default='mysql+pymysql://root:123456@siconvdata:3306/siconvdata')
CHUNK_SIZE = getenv('CHUNK_SIZE', default=500000)

class DB(object):
    def __init__(self) -> None:
        self.engine = None
        self.db_uri = getenv('SQLALCHEMY_DATABASE_URI', 
                                 default='mysql+pymysql://root:123456@siconvdata:3306/siconvdata')

    def connect_database(self):
        self.engine = None
        if database_exists(self.db_uri):
            self.engine = sa.create_engine(self.db_uri)
    
    def check_database(self):
        current_date_table = 'data_atual'
        if sa.inspect(self.engine).has_table(current_date_table):
            return self.engine.execute(f'select DATA_ATUAL from {current_date_table}').scalar()

        raise Exception(f'Tabela {current_date_table} não localizada.')

    def write(self, data_frame, table_name):
        rows_count = 0
        nrows = len(data_frame)
        self.engine.execute(f'truncate table {table_name};')
        for index in range(0, nrows, CHUNK_SIZE):
            df = data_frame[index:index + CHUNK_SIZE]
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            rows_count += len(df)
        return rows_count
    

if __name__ == '__main__':
    path = str(Path(__file__).parent.joinpath('json_loads'))
    
    json_loader = JSONLoader(path=path, logger=getLogger())
    mysql_loader = MySQLLoader(database=DB(), logger=getLogger())
    etl = ETL(loaders=[json_loader, mysql_loader], config=Config(), file_tools=FileTools(), pd=pd)
    etl.pipeline(force_download=False, force_transformations=False)
