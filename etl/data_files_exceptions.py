# coding: utf-8

class FILESUpToDateException(Exception):
    """Custom exception to be raised if data is up to date in data files"""
    
    def __init__(self, expression='', message=''):
        self.expression = expression
        self.message = message

class FILESUnchangedException(Exception):
    """Custom exception to be raised if data was not changed in data files"""
    
    def __init__(self, expression='', message=''):
        self.expression = expression
        self.message = message
