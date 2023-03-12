# -*- coding: utf-8 -*-

class DBUpToDateException(Exception):
    """Custom exception to be raised if data is up to date in database"""
    
    def __init__(self, expression='', message=''):
        self.expression = expression
        self.message = message

class DBUnchangedException(Exception):
    """Custom exception to be raised if data was not changed in database"""
    
    def __init__(self, expression='', message=''):
        self.expression = expression
        self.message = message

class DBNotFoundException(Exception):
    """Custom exception to be raised if database was not found"""
    
    def __init__(self, expression='', message=''):
        self.expression = expression
        self.message = message
