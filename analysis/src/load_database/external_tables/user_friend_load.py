'''
Created on 30/3/2015

@author: lorenzorubio
'''
from abstract_load import Abstract_load

class User_friend_load(Abstract_load):
    '''
    manages external table follower_load
    '''
    
    external_table_drop_query = '''
        drop table follower_load
    '''
    external_table_definition_preformatted = '''
        create table follower_load (
            user_id varchar(256),
            followed_user_id VARCHAR(256)
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
        insert into follower
        (user_id, followed_user_id)
        select distinct user_id, followed_user_id from follower_load
    '''

    def __init__(self, filename, connection):
        '''
        Constructor
        '''
        Abstract_load.__init__(self, connection)
        self.filename = filename
        
    def recreate_external_table(self):
        # change file name in definition
        self.external_table_definition_query = self.external_table_definition_preformatted.format(
            external_table_filename = self.filename
        )
        #print(q)
        self.recreate_external_table_abstract()
        
    def insert_into_target(self):
        # avoid duplicates: disable pk
        self.generic_query("alter table follower disable constraint follower_pk", do_commit=False)
        # insert-select
        Abstract_load.insert_into_target(self, do_commit=False)
        # remove duplicates
        self.generic_query("""
                delete from follower
                where rowid not in (select max(rowid) from follower group by user_id, followed_user_id)
            """, do_commit=True)
        # enable pk
        self.generic_query("alter table follower enable constraint follower_pk", do_commit=False)
            
        
        
        