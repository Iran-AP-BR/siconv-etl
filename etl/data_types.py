# coding: utf-8

import pandas as pd


parse_dates_convenios = ['DIA_ASSIN_CONV', 'DIA_PUBL_CONV', 'DIA_INIC_VIGENC_CONV', 
                         'DIA_FIM_VIGENC_CONV', 'DIA_LIMITE_PREST_CONTAS']
parse_dates_movimento = ['DATA_MOV']
parse_dates_calendario = ['DATA']
parse_dates_data_atual = ['DATA_ATUAL']

tbl_convenios_type = {
    'NR_CONVENIO': 'int64',
    'DIA_ASSIN_CONV': 'object',
    'SIT_CONVENIO': 'object',
    'INSTRUMENTO_ATIVO': 'object',
    'DIA_PUBL_CONV': 'object',
    'DIA_INIC_VIGENC_CONV': 'object',
    'DIA_FIM_VIGENC_CONV': 'object',
    'DIA_LIMITE_PREST_CONTAS': 'object',
    'VL_GLOBAL_CONV': 'float64',
    'VL_REPASSE_CONV': 'float64',
    'VL_CONTRAPARTIDA_CONV': 'float64',
    'COD_ORGAO_SUP': 'int64',
    'DESC_ORGAO_SUP': 'object',
    'NATUREZA_JURIDICA': 'object',
    'COD_ORGAO': 'int64',
    'DESC_ORGAO': 'object',
    'MODALIDADE': 'object',
    'IDENTIF_PROPONENTE': 'object',
    'OBJETO_PROPOSTA': 'object',
    'VALOR_EMENDA_CONVENIO': 'float64',
    'COM_EMENDAS': 'object',
    'INSUCESSO': 'float64'
}

tbl_movimento_type = {
    'MOV_ID': 'int64',
    'NR_CONVENIO': 'int64',
    'DATA_MOV': 'object',
    'VALOR_MOV': 'float64',
    'TIPO_MOV': 'object',
    'FORNECEDOR_ID': 'int64'
}

tbl_fornecedores_type = {
    'FORNECEDOR_ID': 'int64',
    'IDENTIF_FORNECEDOR': 'object',
    'NOME_FORNECEDOR': 'object'
}

tbl_emendas_type_convenios = {
    'NR_EMENDA': 'int64',
    'NR_CONVENIO': 'int64',
    'VALOR_REPASSE_EMENDA': 'float64'
}

tbl_emendas_type = {
    'NR_EMENDA': 'int64',
    'NOME_PARLAMENTAR': 'object',
    'TIPO_PARLAMENTAR': 'object',
    'VALOR_EMENDA': 'float64'
}

tbl_estados_type = {
    'SIGLA': 'object',
    'NOME': 'object',
    'REGIAO': 'object',
    'REGIAO_ABREVIADA': 'object'
}

tbl_municipios_type = {
    'CODIGO_IBGE': 'int64',
    'NOME_MUNICIPIO': 'object',
    'UF': 'object',
    'REGIAO': 'object',
    'REGIAO_ABREVIADA': 'object',
    'NOME_ESTADO': 'object',
    'LATITUDE': 'float64',
    'LONGITUDE': 'float64',
    'CAPITAL': 'object'
}

tbl_proponentes_type = {
    'IDENTIF_PROPONENTE': 'object',
    'NM_PROPONENTE': 'object',
    'CODIGO_IBGE': 'int64'
}

tbl_licitacoes_type = {    
    'ID_LICITACAO': 'int64', 
    'NR_CONVENIO': 'int64', 
    'MODALIDADE_COMPRA': 'object', 
    'TIPO_LICITACAO': 'object', 
    'FORMA_LICITACAO': 'object', 
    'REGISTRO_PRECOS': 'object', 
    'LICITACAO_INTERNACIONAL': 'object',
    'STATUS_LICITACAO': 'object', 
    'VALOR_LICITACAO': 'float64'
}

tbl_calendario_type = {
    'DATA': 'object',
    'ANO': 'int64',
    'MES_NUMERO': 'int64',
    'MES_NOME': 'object',
    'ANO_MES_NUMERO': 'int64',
    'MES_ANO': 'object',
    'MES_NOME_ANO': 'object',
    'TRIMESTRE_NUMERO': 'int64',
    'TRIMESTRE_NOME': 'object',
    'ANO_TRIMESTRE_NUMERO': 'int64',
    'TRIMESTRE_ANO': 'object',
    'TRIMESTRE_NOME_ANO': 'object',
    'SEMANA_NUMERO': 'int64',
    'SEMANA_NOME': 'object',
    'ANO_SEMANA_NUMERO': 'Int64',
    'SEMANA_ANO': 'object',
    'SEMANA_NOME_ANO': 'object',
    'DIA_DA_SEMANA': 'object'
}

tbl_data_atual_type = {
    'DATA_ATUAL': 'object',
}


def set_types(df, dtypes):
    df = df.astype(dtypes)
    if dtypes==tbl_convenios_type:
        df['DIA_ASSIN_CONV'] = pd.to_datetime(df['DIA_ASSIN_CONV'], dayfirst=True)
        df['DIA_PUBL_CONV'] = pd.to_datetime(df['DIA_PUBL_CONV'], dayfirst=True)
        df['DIA_INIC_VIGENC_CONV'] = pd.to_datetime(df['DIA_INIC_VIGENC_CONV'], dayfirst=True)
        df['DIA_FIM_VIGENC_CONV'] = pd.to_datetime(df['DIA_FIM_VIGENC_CONV'], dayfirst=True)
        df['DIA_LIMITE_PREST_CONTAS'] = pd.to_datetime(df['DIA_LIMITE_PREST_CONTAS'], dayfirst=True)
    elif dtypes==tbl_movimento_type:
        df['DATA_MOV'] = pd.to_datetime(df['DATA_MOV'], dayfirst=True)
    elif dtypes==tbl_calendario_type:
        df['DATA'] = pd.to_datetime(df['DATA'], dayfirst=True)
    elif dtypes==tbl_data_atual_type:
        df['DATA_ATUAL'] = pd.to_datetime(df['DATA_ATUAL'], dayfirst=True)

    return df