# -*- coding: utf-8 -*-

from pathlib import Path
from datetime import datetime
from .utils import *


class Extraction(object):
    def __init__(self, logger, current_date, config, file_tools, pd) -> None:
        self.config = config
        self.logger = logger
        self.current_date = current_date
        self.file_tools = file_tools
        self.pd = pd


    def extract(self, force_download=False):

        self.logger.info('[Extracting data]')

        self.__fetch_estados__(force_download=force_download)
        self.__fetch_municipios__(force_download=force_download)
        self.__fetch_proponentes__(force_download=force_download)
        self.__fetch_propostas__(force_download=force_download)
        self.__fetch_convenios__(force_download=force_download)
        self.__fetch_emendas__(force_download=force_download)
        self.__fetch_desembolsos__(force_download=force_download)
        self.__fetch_contrapartidas__(force_download=force_download)
        self.__fetch_tributos__(force_download=force_download)
        self.__fetch_pagamentos__(force_download=force_download)
        self.__fetch_obtv__(force_download=force_download)
        self.__fetch_licitacoes__(force_download=force_download)

        self.logger.info('[Extraction complete]')


    def __already_extracted__(self, table_name, date_verification=True):
        result = False
        file = Path(self.config.STAGE_FOLDER).joinpath(f'{table_name}{self.config.FILE_EXTENTION}')
        if file.exists():
            if date_verification:
                creation_date = datetime.fromtimestamp(file.stat().st_mtime).date()
                result = True if creation_date >= self.current_date else False
            else:
                result = True

        return result


    def __fetch_estados__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='estados'):
            feedback(self.logger, label='-> estados', value='connecting...')
            
            estados_extract_cols = ["codigo_uf", "uf", "nome", "regiao"]
            
            estados = self.pd.read_csv(f'{self.config.MUNICIPIOS_BACKUP_FOLDER}/estados.csv.gz', 
                                    compression='gzip', sep=',', dtype=str,
                                    usecols=estados_extract_cols).drop_duplicates()
            
            self.file_tools.write_to_stage(table=estados, table_name='estados', current_date=self.current_date)
            
            feedback(self.logger, label='-> estados', value=f'{rows_print(estados)}')
        else:
            feedback(self.logger, label='-> estados', value='up to date')


    def __fetch_municipios__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='municipios'):
            feedback(self.logger, label='->municípios', value='connecting...')
            
            
            municipios_extract_cols = ["codigo_ibge", "nome", "latitude", "longitude", "capital", "codigo_uf"]
            
            municipios = self.pd.read_csv(f'{self.config.MUNICIPIOS_BACKUP_FOLDER}/municipios.csv.gz', 
                                        compression='gzip', sep=',', dtype=str,
                                        usecols=municipios_extract_cols).drop_duplicates()
            
            self.file_tools.write_to_stage(table=municipios, table_name='municipios', 
                                      current_date=self.current_date)
            
            feedback(self.logger, label='-> municípios', value=f'{rows_print(municipios)}')
        else:
            feedback(self.logger, label='-> municípios', value='up to date')

    def __fetch_proponentes__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='proponentes'):
            feedback(self.logger, label='-> proponentes', value='connecting...')
            
            
            proponentes_extract_cols = ["IDENTIF_PROPONENTE", "NM_PROPONENTE"]
            
            proponentes = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_proponentes.csv.zip', 
                                    compression='zip', sep=';', dtype=str, 
                                    usecols=proponentes_extract_cols).drop_duplicates()
            
            self.file_tools.write_to_stage(table=proponentes, table_name='proponentes', 
                                      current_date=self.current_date)
            
            feedback(self.logger, label='-> proponentes', value=f'{rows_print(proponentes)}')
        else:
            feedback(self.logger, label='-> proponentes', value='up to date')

    def __fetch_propostas__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='propostas'):
            feedback(self.logger, label='-> propostas', value='connecting...')
            
            
            propostas_extract_cols = ["ID_PROPOSTA", "COD_MUNIC_IBGE", "COD_ORGAO_SUP", "DESC_ORGAO_SUP",
                                    "NATUREZA_JURIDICA", "COD_ORGAO", "DESC_ORGAO", "MODALIDADE", 
                                    "IDENTIF_PROPONENTE", "OBJETO_PROPOSTA", 'ANO_PROP', 'UF_PROPONENTE', 
                                    'MUNIC_PROPONENTE']
            
            propostas = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_proposta.csv.zip', 
                                    compression='zip', sep=';', dtype=str, 
                                    usecols=propostas_extract_cols).drop_duplicates()
            
            self.file_tools.write_to_stage(table=propostas, table_name='propostas', current_date=self.current_date)
            
            feedback(self.logger, label='-> propostas', value=f'{rows_print(propostas)}') 
        else:
            feedback(self.logger, label='-> propostas', value='up to date') 

    def __fetch_convenios__(self, force_download=False):
        
        if force_download or not self.__already_extracted__(table_name='convenios'):
            feedback(self.logger, label='-> convênios', value='connecting...')
    
            
            convenios_extract_cols = ["NR_CONVENIO", "ID_PROPOSTA", "DIA_ASSIN_CONV", "SIT_CONVENIO", 
                                      "INSTRUMENTO_ATIVO", "DIA_PUBL_CONV", "DIA_INIC_VIGENC_CONV", 
                                      "DIA_FIM_VIGENC_CONV", "DIA_LIMITE_PREST_CONTAS", "VL_GLOBAL_CONV",
                                       "VL_REPASSE_CONV", "VL_CONTRAPARTIDA_CONV"]
            
            convenios = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_convenio.csv.zip', 
                                    compression='zip', sep=';', dtype=str, usecols=convenios_extract_cols)
            convenios = convenios[(convenios['DIA_ASSIN_CONV'].notna()) & (convenios['DIA_PUBL_CONV'].notna())]
            convenios.loc[convenios['INSTRUMENTO_ATIVO'].str.upper()=='NÃO', ['INSTRUMENTO_ATIVO']] = 'NAO'
            convenios = convenios.drop_duplicates()
    
            self.file_tools.write_to_stage(table=convenios, table_name='convenios', 
                                      current_date=self.current_date)
    
            feedback(self.logger, label='-> convênios', value=f'{rows_print(convenios)}')
        else:
            feedback(self.logger, label='-> convênios', value='up to date') 

    def __fetch_emendas__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='emendas'):
            feedback(self.logger, label='-> emendas', value='connecting...')
            
            
            emendas_extract_cols = ['ID_PROPOSTA', 'NR_EMENDA', 'NOME_PARLAMENTAR', 
                                    'TIPO_PARLAMENTAR', 'VALOR_REPASSE_EMENDA']
            
            emendas = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_emenda.csv.zip', 
                                compression='zip', sep=';', dtype=str, 
                                usecols=emendas_extract_cols).drop_duplicates()
            
            self.file_tools.write_to_stage(table=emendas, table_name='emendas', 
                                      current_date=self.current_date)
            
            feedback(self.logger, label='-> emendas', value=f'{rows_print(emendas)}')
        else:
            feedback(self.logger, label='-> emendas', value='up to date') 

    def __fetch_desembolsos__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='desembolsos'):
            feedback(self.logger, label='-> desembolsos', value='connecting...')

            
            desembolsos_extract_cols = ["ID_DESEMBOLSO", "NR_CONVENIO", "DATA_DESEMBOLSO", 
                                        "VL_DESEMBOLSADO"]
           
            desembolsos = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_desembolso.csv.zip', 
                                    compression='zip', sep=';', dtype=str, 
                                    usecols=desembolsos_extract_cols).drop_duplicates()
            
            self.file_tools.write_to_stage(table=desembolsos, table_name='desembolsos', 
                                      current_date=self.current_date)
            
            feedback(self.logger, label='-> desembolsos', value=f'{rows_print(desembolsos)}')
        else:
            feedback(self.logger, label='-> desembolsos', value='up to date') 

    def __fetch_contrapartidas__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='contrapartidas'):
            feedback(self.logger, label='-> contrapartidas', value='connecting...')

            
            contrapartidas_extract_cols = ["NR_CONVENIO", "DT_INGRESSO_CONTRAPARTIDA", 
                                           "VL_INGRESSO_CONTRAPARTIDA"]

            contrapartidas = self.pd.read_csv(
                             f'{self.config.DOWNLOAD_URI}/siconv_ingresso_contrapartida.csv.zip', 
                             compression='zip', sep=';', dtype=str,
                             usecols=contrapartidas_extract_cols).drop_duplicates()
            
            self.file_tools.write_to_stage(table=contrapartidas, table_name='contrapartidas', 
                                      current_date=self.current_date)
            
            feedback(self.logger, label='-> contrapartidas', value=f'{rows_print(contrapartidas)}')
        else:
            feedback(self.logger, label='-> contrapartidas', value='up to date') 

    def __fetch_tributos__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='tributos'):
            feedback(self.logger, label='-> tributos', value='connecting...')

            
            tributos_extract_cols = ["NR_CONVENIO", "DATA_TRIBUTO", "VL_PAG_TRIBUTOS"]

            tributos = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_pagamento_tributo.csv.zip', 
                                        compression='zip', sep=';', dtype=str,
                                        usecols=tributos_extract_cols).drop_duplicates()

            self.file_tools.write_to_stage(table=tributos, table_name='tributos', 
                                      current_date=self.current_date)
            
            feedback(self.logger, label='-> tributos', value=f'{rows_print(tributos)}')
        else:
            feedback(self.logger, label='-> tributos', value='up to date') 

    def __fetch_pagamentos__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='pagamentos'):
            feedback(self.logger, label='-> pagamentos', value='connecting...')

            
            pagamentos_extract_cols = ["NR_MOV_FIN", "NR_CONVENIO", "IDENTIF_FORNECEDOR", 
                                       "NOME_FORNECEDOR", "DATA_PAG", "VL_PAGO"]
            
            pagamentos = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_pagamento.csv.zip', 
                                    compression='zip', sep=';', dtype=str, 
                                    usecols=pagamentos_extract_cols).drop_duplicates()
            
            self.file_tools.write_to_stage(table=pagamentos, table_name='pagamentos', 
                                      current_date=self.current_date)
            
            feedback(self.logger, label='-> pagamentos', value=f'{rows_print(pagamentos)}')
        else:
            feedback(self.logger, label='-> pagamentos', value='up to date') 

    def __fetch_obtv__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='obtv'):
            feedback(self.logger, label='-> OBTV', value='connecting...')

            
            obtv_extract_cols = ["NR_MOV_FIN", "IDENTIF_FAVORECIDO_OBTV_CONV", 
                                 "NM_FAVORECIDO_OBTV_CONV", "TP_AQUISICAO", "VL_PAGO_OBTV_CONV"]

            obtv = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_obtv_convenente.csv.zip', 
                               compression='zip', sep=';', dtype=str, usecols=obtv_extract_cols)
            obtv = obtv[obtv['IDENTIF_FAVORECIDO_OBTV_CONV'].notna()]
            obtv = obtv.drop_duplicates()
            
            self.file_tools.write_to_stage(table=obtv, table_name='obtv', current_date=self.current_date)
            
            feedback(self.logger, label='-> OBTV', value=f'{rows_print(obtv)}')
        else:
            feedback(self.logger, label='-> OBTV', value='up to date') 

    def __fetch_licitacoes__(self, force_download=False):
        if force_download or not self.__already_extracted__(table_name='licitacoes'):
            feedback(self.logger, label='-> licitações', value='connecting...')

            
            licitacoes_extract_cols=['ID_LICITACAO', 'NR_CONVENIO', 'MODALIDADE_LICITACAO',
                                     'TP_PROCESSO_COMPRA', 'TIPO_LICITACAO', 'STATUS_LICITACAO', 
                                     'VALOR_LICITACAO']
            
            licitacoes = self.pd.read_csv(f'{self.config.DOWNLOAD_URI}/siconv_licitacao.csv.zip', 
                                     compression='zip', sep=';', dtype=str, 
                                     usecols=licitacoes_extract_cols)
            
            self.file_tools.write_to_stage(table=licitacoes, table_name='licitacoes', 
                                      current_date=self.current_date)
            
            feedback(self.logger, label='-> licitações', value=f'{rows_print(licitacoes)}')
        else:
            feedback(self.logger, label='-> licitações', value='up to date') 