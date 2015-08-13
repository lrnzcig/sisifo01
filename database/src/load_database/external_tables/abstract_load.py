'''
Created on 30/3/2015

@author: lorenzorubio
'''

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
        self.sconnection.generic_query(self.external_table_drop_query, do_commit=False)
        self.sconnection.generic_query(self.external_table_definition_query, do_commit=False)
        
    
    def insert_into_target(self, do_commit=True):
        self.sconnection.generic_query(self.insert_select_query, do_commit)

    def generic_query(self, query, do_commit=True):
        self.sconnection.generic_query(query, do_commit)
          
        
        
        