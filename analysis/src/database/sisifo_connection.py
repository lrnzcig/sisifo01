'''
Created on 1 de abr. de 2015

@author: lorenzorubio
'''
import cx_Oracle as oracle
from os.path import expanduser
import yaml

class SisifoConnection():
    '''
    Handles connection for tables being loaded
    '''
    
    def __init__(self):
        properties = yaml.load(open(expanduser("~") + '/.sisifo/connection.properties'))
        self.database = properties['database']
        self.connection = oracle.connect(self.database)
    
    def get(self):
        return self.connection
    
    def close(self):
        self.connection.close()

class OracleErrorList():
    TABLE_DOES_NOT_EXIST_ERROR = 942
    DUPLICATE_PK_ERROR = 1
        