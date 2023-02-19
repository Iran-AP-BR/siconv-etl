# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from .data_files_tools import FileTools

class LoaderClass(ABC):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        self.file_tools = FileTools()
    
    @abstractmethod
    def load(self) -> None:
        pass

    def read_data(self, table_name) -> any:
        return self.file_tools.read_data(table_name=table_name)