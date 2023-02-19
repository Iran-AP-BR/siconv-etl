# -*- coding: utf-8 -*-

import pandas as pd
import pickle
import numpy as np
from etl.config import Config
from .text_transformer import TextTransformer

class Unpickler_(pickle.Unpickler):
    def find_class(self, module, name):
        if module == "__main__":
            module = __name__
        return super().find_class(module, name)


class MLModel(object):

    def __init__(self, classifier, transformers, principal_components_analysis,
                 validation_metrics, hyperparameters, sklearn_version=None):

        self.transformers = transformers
        self.principal_components_analysis = principal_components_analysis
        self.classifier = classifier
        self.validation_metrics = validation_metrics
        self.hyperparameters = hyperparameters
        self.sklearn_version = sklearn_version


class RiskAnalyzer(object):

    def __init__(self, ylabel_name = 'INSUCESSO'):

        self.ylabel_name = ylabel_name

        self.config = Config()
        self.__model_filename_path__ = self.config.MODEL_PATH

        self.__model_object__ = self.__load_model__()

        self.__ibge__ = None
        self.__principais_parlamentares__ = None
        self.__principais_fornecedores__ = None

    def validation_metrics(self):
        return self.__model_object__.validation_metrics

    def hyperparameters(self):
        return self.__model_object__.hyperparameters

    def sklearn_version(self):
        return self.__model_object__.sklearn_version

    def predict(self, X_conv, proba=False, scale=False, append=False):
        dataframe_type = True
        if not isinstance(X_conv, pd.DataFrame):
            X= pd.DataFrame(X_conv)
            dataframe_type = False
        else:
            X = X_conv.copy()

        nr_convenio = None
        if 'NR_CONVENIO' in X.columns:
            nr_convenio = X.pop('NR_CONVENIO').to_frame()

        X = self.__data_preparation__(X)
        X = self.__model_object__.principal_components_analysis.transform(X)
        if proba:
            predictions = self.__model_object__.classifier.predict_proba(X)[:, 1]
            if scale:
                predictions = np.array(list(map(lambda value: self.__sigmoid__(value), predictions)))
        else:
            predictions = self.__model_object__.classifier.predict(X)

        predictions = pd.Series(predictions, name=self.ylabel_name)

        if append:
            predictions = pd.DataFrame(predictions)
            predictions = pd.concat([X, predictions], axis=1, sort=False, ignore_index=False)

            if isinstance(nr_convenio, pd.DataFrame):
                predictions = pd.concat([nr_convenio, predictions], axis=1, sort=False, ignore_index=False)

        if not dataframe_type:
            predictions = predictions.to_dict()

        return predictions

    def run(self, convenios, proponentes, emendas, emendas_convenios, fornecedores, movimento, append=False):

        convenios_ = self.__transform_dataset__(convenios, proponentes, emendas, emendas_convenios, fornecedores, movimento)

        return self.predict(convenios_, proba=True, scale=True, append=append)

    def __sigmoid__(self, value):

        middle_value = 0.5
        slope_factor = 2
        return 1/(1+np.e**(-slope_factor*(value-middle_value)))

    def __load_model__(self):
        with open(self.__model_filename_path__, 'rb') as fd:
            model_object = Unpickler_(fd).load()

        return model_object

    def __data_preparation__(self, X):

        data = X.copy()
        transformers = self.__model_object__.transformers
        data['OBJETO_PROPOSTA'] = transformers['TEXT_CLUSTERER'].predict(data['OBJETO_PROPOSTA'])
        data['OBJETO_PROPOSTA'] = data['OBJETO_PROPOSTA'].astype('int64')

        data_categorical_parlamentar = data.pop('PRINCIPAL_PARLAMENTAR').to_frame()
        data_categorical_fornecedor = data.pop('PRINCIPAL_FORNECEDOR').to_frame()

        data_categorical_object = data.select_dtypes(include=['object'])
        data_categorical_int = data.select_dtypes(include=['int64'])
        data_value = data.select_dtypes(include='float64')

        value_codes = transformers['VALUE'].transform(data_value)
        value_feature_names = transformers['VALUE'].feature_names_in_
        data_value = pd.DataFrame(value_codes, columns=value_feature_names).astype('float64')

        categorical_object_codes = transformers['CATEGORICAL_OBJECT'].transform(data_categorical_object).toarray()
        categorical_object_feature_names= transformers['CATEGORICAL_OBJECT'].get_feature_names_out()
        data_categorical_object = pd.DataFrame(categorical_object_codes, columns=categorical_object_feature_names).astype('float64')

        categorical_int_codes = transformers['CATEGORICAL_INT'].transform(data_categorical_int).toarray()
        categorical_int_feature_names= transformers['CATEGORICAL_INT'].get_feature_names_out()
        data_categorical_int = pd.DataFrame(categorical_int_codes, columns=categorical_int_feature_names).astype('float64')

        parlamentar_codes = transformers['CATEGORICAL_PARLAMENTAR'].transform(data_categorical_parlamentar).toarray()
        parlamentar_feature_names= transformers['CATEGORICAL_PARLAMENTAR'].get_feature_names_out()
        data_categorical_parlamentar = pd.DataFrame(parlamentar_codes, columns=parlamentar_feature_names).astype('float64')

        fornecedor_codes = transformers['CATEGORICAL_FORNECEDOR'].transform(data_categorical_fornecedor).toarray()
        fornecedor_feature_names= transformers['CATEGORICAL_FORNECEDOR'].get_feature_names_out()
        data_categorical_fornecedor = pd.DataFrame(fornecedor_codes, columns=fornecedor_feature_names).astype('float64')

        return pd.concat([data_value, data_categorical_object, data_categorical_int,
                          data_categorical_parlamentar, data_categorical_fornecedor], axis=1, sort=False)


    def __transform_dataset__(self, convenios, proponentes, emendas, emendas_convenios, fornecedores, movimento, ylabel=False):

        self.__ibge__ = proponentes[['IDENTIF_PROPONENTE', 'CODIGO_IBGE']].copy()

        selected_columns = ['VL_REPASSE_CONV', 'VL_CONTRAPARTIDA_CONV', 'VALOR_EMENDA_CONVENIO',
               'OBJETO_PROPOSTA', 'COD_ORGAO', 'COD_ORGAO_SUP', 'NATUREZA_JURIDICA',
               'MODALIDADE', 'IDENTIF_PROPONENTE', 'COM_EMENDAS']

        features_columns = ['NR_CONVENIO', *selected_columns]
        if ylabel:
            features_columns += [self.ylabel_name]

        convenios_ = convenios[features_columns].copy()

        self.__principais_parlamentares__ = self.__get_principais_parlamentares__(emendas=emendas, emendas_convenios=emendas_convenios, convenios_list=convenios_['NR_CONVENIO'].to_list())
        self.__principais_fornecedores__ = self.__get_principais_fornecedores__(movimento=movimento, fornecedores=fornecedores, convenios_list=convenios_['NR_CONVENIO'].to_list())

        dataset = pd.merge(convenios_, self.__ibge__, how='inner', on=['IDENTIF_PROPONENTE'], left_index=False, right_index=False)

        dataset = pd.merge(dataset, self.__principais_parlamentares__, how='left', on=['NR_CONVENIO'], left_index=False, right_index=False)

        dataset = pd.merge(dataset, self.__principais_fornecedores__, how='left', on=['NR_CONVENIO'], left_index=False, right_index=False)

        dataset = dataset.fillna('NAO APLICAVEL')

        Xdtypes = {'VL_REPASSE_CONV': 'float64', 'VL_CONTRAPARTIDA_CONV': 'float64',
                   'VALOR_EMENDA_CONVENIO': 'float64', 'OBJETO_PROPOSTA': 'object',
                   'COD_ORGAO': 'int64', 'COD_ORGAO_SUP': 'int64', 'NATUREZA_JURIDICA': 'object',
                   'MODALIDADE': 'object', 'IDENTIF_PROPONENTE': 'object', 'COM_EMENDAS': 'object',
                   'CODIGO_IBGE': 'int64', 'PRINCIPAL_PARLAMENTAR': 'object', 'PRINCIPAL_FORNECEDOR': 'object'}

        return dataset.astype(Xdtypes)

    def __get_principais_parlamentares__(self, emendas, emendas_convenios, convenios_list):

        convenios_repasses_emendas = emendas_convenios.loc[emendas_convenios['NR_CONVENIO'].isin(convenios_list)].copy()
        convenios_repasses_emendas['rank'] = convenios_repasses_emendas.groupby(by=['NR_CONVENIO'])['VALOR_REPASSE_EMENDA'].rank(ascending=False, method='min')
        convenios_repasses_emendas = convenios_repasses_emendas.loc[convenios_repasses_emendas['rank']==1, ['NR_CONVENIO', 'NR_EMENDA']]
        convenios_parlamentares = pd.merge(convenios_repasses_emendas, emendas, on=['NR_EMENDA'], left_index=False, right_index=False)
        convenios_parlamentares = convenios_parlamentares[['NR_CONVENIO', 'NOME_PARLAMENTAR']]
        convenios_parlamentares.columns = ['NR_CONVENIO', 'PRINCIPAL_PARLAMENTAR']
        convenios_parlamentares = convenios_parlamentares.groupby(by=['NR_CONVENIO']).max().reset_index()

        return convenios_parlamentares

    def __get_principais_fornecedores__(self, movimento, fornecedores, convenios_list):

        movimento_exec = movimento.loc[movimento['NR_CONVENIO'].isin(convenios_list)].copy()
        movimento_exec = movimento_exec[movimento_exec['TIPO_MOV']=='P']
        convenios_fornecedores = movimento_exec[['NR_CONVENIO', 'FORNECEDOR_ID', 'VALOR_MOV']].groupby(by=['NR_CONVENIO', 'FORNECEDOR_ID']).sum().reset_index().copy()
        convenios_fornecedores['rank'] = convenios_fornecedores.groupby(by=['NR_CONVENIO'])['VALOR_MOV'].rank(ascending=False, method='min')
        convenios_fornecedores = convenios_fornecedores.loc[convenios_fornecedores['rank']==1, ['NR_CONVENIO', 'FORNECEDOR_ID']]
        convenios_fornecedores = pd.merge(convenios_fornecedores, fornecedores, on=['FORNECEDOR_ID'], left_index=False, right_index=False)
        convenios_fornecedores = convenios_fornecedores.sort_values(['NR_CONVENIO', 'IDENTIF_FORNECEDOR'], ascending=False)
        convenios_fornecedores = convenios_fornecedores[['NR_CONVENIO', 'IDENTIF_FORNECEDOR']]
        convenios_fornecedores.columns = ['NR_CONVENIO', 'PRINCIPAL_FORNECEDOR']
        convenios_fornecedores = convenios_fornecedores.groupby(by=['NR_CONVENIO']).max().reset_index()

        return convenios_fornecedores

