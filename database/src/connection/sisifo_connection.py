'''
Created on 1 de abr. de 2015

@author: lorenzorubio
'''
import cx_Oracle
from os.path import expanduser
import yaml

class SisifoConnection():
    '''
    Handles connection for tables being loaded
    '''
    
    def __init__(self, user=None):
        with open(expanduser("~") + '/.sisifo/connection.properties') as f:
            properties = yaml.load(f)
            self.database = properties['database']
            dsnStr = cx_Oracle.makedsn(self.database['host'], self.database['port'], self.database['sid'])
            if user == None:
                user = self.database['user']
            self.connection = cx_Oracle.connect(user=user, password=self.database['password'], dsn=dsnStr)
    
    def get(self):
        return self.connection
    
    def close(self):
        self.connection.close()
    
    def generic_query(self, query, do_commit=True):
        cur = self.connection.cursor()
        try:
            cur.execute(query)
        except cx_Oracle.DatabaseError as e:
            self.close_cursor(cur)
            try:
                self.connection.rollback()
            except cx_Oracle.DatabaseError:
                pass
            error, = e.args
            if (error.code != OracleErrorList.TABLE_DOES_NOT_EXIST_ERROR) & (error.code != OracleErrorList.OBJECT_DOES_NOT_EXIST_ERROR):
                raise RuntimeError(error)
            return
        else:
            SisifoConnection.close_cursor(self, cur)
            if (do_commit == True):
                self.connection.commit()
            return
        
    def close_cursor(self, cursor):
        try:
            cursor.close()
        except cx_Oracle.DatabaseError:
            pass


class OracleErrorList():
    TABLE_DOES_NOT_EXIST_ERROR = 942
    OBJECT_DOES_NOT_EXIST_ERROR = 4043    
    DUPLICATE_PK_ERROR = 1
        