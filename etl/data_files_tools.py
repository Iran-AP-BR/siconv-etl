# -*- coding: utf-8 -*-

import pandas as pd
import os
from pathlib import Path
from .utils import datetime_validation
from datetime import datetime
from etl.config import Config


class FileTools(object):
    def __init__(self) -> None:
        self.config = Config()
        Path(self.config.STAGE_FOLDER).mkdir(parents=True, exist_ok=True)
        Path(self.config.DATA_FOLDER).mkdir(parents=True, exist_ok=True)


    def set_file_timestamp(self, table_name, current_date, type):
        assert type in ('stage', 'end'), "type must be 'stage' or 'end'"

        if type=='stage':
            file_path = Path(self.config.STAGE_FOLDER).joinpath(f'{table_name}{self.config.FILE_EXTENTION}')
        else:
            file_path = Path(self.config.DATA_FOLDER).joinpath(f'{table_name}{self.config.FILE_EXTENTION}')

        dt = datetime(current_date.year, current_date.month, current_date.day, -self.config.TIMEZONE_OFFSET, 0)
        os.utime(file_path, (dt.timestamp(), dt.timestamp()))


    def get_files_date(self, with_exception=False):
        try:
            data_atual = self.read_data(table_name='data_atual')['DATA_ATUAL'][0]
            return datetime_validation(str(data_atual), dayfirst=False)
        except:
            if with_exception:
                raise Exception(f'Não foi possível obter a data atual.')
            else:
                return None

    def __strip_white_spaces__(self, dataframe):
        df_str = dataframe.select_dtypes(['object'])
        dataframe[df_str.columns] = df_str.apply(lambda x: x.str.strip())
        return dataframe
        

    def write_data(self, table, table_name, current_date=None):
        table.to_parquet(Path(self.config.DATA_FOLDER).joinpath(f'{table_name}{self.config.FILE_EXTENTION}'),
                            compression=self.config.COMPRESSION_METHOD, index=False)
        
        if current_date is not None:
            self.set_file_timestamp(table_name=table_name, current_date=current_date, type='end')


    def read_data(self, table_name):
        return pd.read_parquet(Path(self.config.DATA_FOLDER).joinpath(f'{table_name}{self.config.FILE_EXTENTION}'))

    def read_from_stage(self, table_name):
        return pd.read_parquet(Path(self.config.STAGE_FOLDER).joinpath(f'{table_name}{self.config.FILE_EXTENTION}'))

    def write_to_stage(self, table, table_name, current_date=None):
        table = self.__strip_white_spaces__(table)
        table.to_parquet(Path(self.config.STAGE_FOLDER).joinpath(f'{table_name}{self.config.FILE_EXTENTION}'),
                            compression=self.config.COMPRESSION_METHOD, index=False)

        if current_date is not None:
            self.set_file_timestamp(table_name=table_name, current_date=current_date, type='stage')

