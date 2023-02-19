# coding: utf-8

from loaders.json_loader import JSONLoader
from etl import ETL
from pathlib import Path


if __name__ == '__main__':
    path = str(Path(__file__).parent.joinpath('json_loads'))
    
    loader = JSONLoader(path=path)
    etl = ETL(loader=loader)
    etl.pipeline(force_download=False, force_transformations=False)
