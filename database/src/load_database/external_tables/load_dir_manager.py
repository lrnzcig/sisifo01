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
    create directory load_dir as '{load_path}/{relative_path}'
    """

    def __init__(self, path, connection, vm_load_path='/media/sf_extsisifo'):
        '''
        Input
            path: directory path / filename (only the filename will be taken into account)
            connection: sisifo connection object
            vm_load_path: final directory
        '''
        Abstract_load.__init__(self, connection)
        self.path = path
        self.vm_load_path = vm_load_path
    
    def drop(self):
        self.generic_query("drop directory load_dir", do_commit=False)

    def create(self):
        head, tail = path.split(self.path)
        if (not tail):
            head, tail = path.split(head)
        q = Load_dir.create_dir_query.format(
            relative_path = tail,
            load_path = self.vm_load_path
        )
        self.generic_query(q, do_commit=False)
