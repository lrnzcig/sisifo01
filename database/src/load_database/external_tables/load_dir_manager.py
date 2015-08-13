'''
Created on 2 de abr. de 2015

@author: lorenzorubio
'''
from load_database.external_tables.abstract_load import Abstract_load
from os import path

class Load_dir(Abstract_load):
    '''
    Manages drop and create of load directory
    '''

    create_dir_query = """
    create directory load_dir as '/media/sf_sisifo01/{relative_path}'
    """

    def __init__(self, path, connection):
        '''
        Input
            path: directory path
            connection: sisifo connection object
        '''
        Abstract_load.__init__(self, connection)
        self.path = path
    
    def drop(self):
        self.generic_query("drop directory load_dir", do_commit=False)

    def create(self):
        head, tail = path.split(self.path)
        if (not tail):
            head, tail = path.split(head)
        q = Load_dir.create_dir_query.format(
            relative_path = tail
        )
        self.generic_query(q, do_commit=False)
