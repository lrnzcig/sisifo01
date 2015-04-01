'''
Created on 30/3/2015

@author: lorenzorubio
'''
import cx_Oracle
from database.sisifo_connection import OracleErrorList


class Abstract_load():
    '''
    manages external tables in general
    
    child objects need to have in self:
    - external_table_drop_query
    - external_table_definition_query
    - insert_select_query
    '''
    
    def __init__(self, connection):
        '''
        Constructor
        '''
        self.sconnection = connection

        
    def recreate_external_table_abstract(self):
        cur = self.sconnection.get().cursor()
        try:
            try:
                cur.execute(self.external_table_drop_query)
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                if (error.code != OracleErrorList.TABLE_DOES_NOT_EXIST_ERROR):
                    raise RuntimeError(error)
            cur.execute(self.external_table_definition_query)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            #self.sconnection.get().rollback()
            raise RuntimeError(error)
        else:
            #self.sconnection.get().commit()
            return
        
    def insert_into_target(self, do_commit=True):
        Abstract_load.generic_query(self, self.insert_select_query, do_commit)
        return

    def generic_query(self, query, do_commit=True):
        cur = self.sconnection.get().cursor()
        try:
            cur.execute(query)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            self.sconnection.get().rollback()
            raise RuntimeError(error)
        else:
            if (do_commit == True):
                self.sconnection.get().commit()
            return
        
        
        
        