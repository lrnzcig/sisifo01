'''
Created on 30/3/2015

@author: lorenzorubio
'''
from abstract_load import Abstract_load

class User_mention_load(Abstract_load):
    '''
    manages external table tusermention_load
    '''
    
    external_table_drop_query = '''
        drop table tusermention_load
    '''
    external_table_definition_preformatted = '''
        create table tusermention_load (
            tweet_id VARCHAR(256),
            source_user_id VARCHAR(256),
            target_user_id VARCHAR(256)
            )
        organization external
            (type oracle_loader
             default directory load_dir
             access parameters
             (records delimited by newline skip 1
              characterset utf8
              FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY "\'"
              LRTRIM
             )
            location ('{external_table_filename}')
            )
        reject limit unlimited
    '''
    insert_select_query = '''
        insert into tusermention
        select distinct tweet_id, source_user_id, target_user_id from tusermention_load
    '''

    def __init__(self, filename, connection):
        '''
        Constructor
        '''
        Abstract_load.__init__(self, connection)
        self.filename = filename
        
    def recreate_external_table(self):
        # change file name in definition
        self.external_table_definition_query = User_mention_load.external_table_definition_preformatted.format(
            external_table_filename = self.filename
        )
        #print(q)
        Abstract_load.recreate_external_table_abstract(self)
        
    def insert_into_target(self):
        Abstract_load.insert_into_target(self)
            
        
        
        