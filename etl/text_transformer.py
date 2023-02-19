# -*- coding: utf-8 -*-

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.base import BaseEstimator, TransformerMixin
import nltk
import re


class TextTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, n_clusters=5, stop_words=[], accented=[]):

        self.__n_clusters__ = n_clusters
        self.__stop_words__ = stop_words
        self.__accented__ = accented
        self.labels_ = None
        self.__vectorizer__ = TfidfVectorizer(use_idf=True)
        self.__clusterer__ = KMeans(n_clusters=self.__n_clusters__, random_state=0)

    def fit(self, X):

        X = self.__preprocessing__(X)
        X = self.__vectorizer__.fit_transform(X)
        self.__clusterer__ = self.__clusterer__.fit(X)
        self.labels_ = self.__clusterer__.labels_
        return self

    def fit_transform(self, X):

        X = self.__preprocessing__(X)
        X = self.__vectorizer__.fit_transform(X)
        X = self.__clusterer__.fit_transform(X)
        self.labels_ = self.__clusterer__.labels_
        return X

    def fit_predict(self, X):

        X = self.__preprocessing__(X)
        X = self.__vectorizer__.fit_transform(X)
        y = self.__clusterer__.fit_predict(X)
        self.labels_ = self.__clusterer__.labels_
        return y

    def transform(self, X):

        X = self.__preprocessing__(X)
        X = self.__vectorizer__.transform(X)
        X = self.__clusterer__.transform(X)
        return X

    def predict(self, X):

        return_unique = False
        if type(X)==str:
            X = [X]
            return_unique = True

        X = self.__preprocessing__(X)
        X = self.__vectorizer__.transform(X)
        y = self.__clusterer__.predict(X)
        y = y[0] if return_unique else y
        return y

    def __preprocessing__(self, X):

        X = [' '.join(self.__text_preprocessing__(t)) for t in X]
        return X

    def __stemming__(self, tokens):

        stemmer = nltk.stem.RSLPStemmer()
        return [stemmer.stem(token) for token in tokens]

    def __remove_accents__(self, text):

        for idx in self.__accented__.index:
            text = text.replace(self.__accented__['char_acc'][idx], self.__accented__['char_norm'][idx])
        return text

    def __text_preprocessing__(self, text):

        text = text.lower()
        text = self.__remove_accents__(text)
        tokens = re.findall('[a-z]+', text)
        tokens = filter(lambda w: w is not None, map(lambda w: None if w in self.__stop_words__ or len(w)==1 else w , tokens))
        tokens = self.__stemming__(tokens)
        return tokens