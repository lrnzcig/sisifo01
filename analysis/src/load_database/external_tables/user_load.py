'''
Created on 30/3/2015

@author: lorenzorubio
'''
from abstract_load import Abstract_load

class User_load(Abstract_load):
    '''
    manages external table tuser_load
    '''
    
    external_table_drop_query = '''
        drop table tuser_load
    '''
    ################################################
    ## IMPORTANT !!!
    ## friends_count not retrieved in all data files
    ## friends_count    VARCHAR(256),
    external_table_definition_preformatted = '''
        create table tuser_load (
            contributors_enabled VARCHAR(256),
            created_at VARCHAR(256),
            description    VARCHAR(1024),
            favourites_count VARCHAR(256),
            followers_count VARCHAR(256),
            id VARCHAR(256),
            is_translator    VARCHAR(256),
            listed_count VARCHAR(256),
            location VARCHAR(256),
            name VARCHAR(256),
            protected    VARCHAR(256),
            screen_name    VARCHAR(256),
            statuses_count VARCHAR(256),
            url    VARCHAR(1024),
            verified VARCHAR(256),
            withheld VARCHAR(256)
        ) organization external
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
    insert into tuser
        (contributors_enabled, created_at, description, favourites_count, followers_count, 
        -- friends_count,
        id, is_translator, listed_count, location, name, protected, screen_name, statuses_count, url, verified, withheld)
    select
        BOOLEAN2CHAR(contributors_enabled),
        to_timestamp_tz(created_at, 'DY MON DD HH24:MI:SS TZHTZM YYYY', 'NLS_DATE_LANGUAGE = AMERICAN') AT time zone 'CET',
        description,
        favourites_count,
        followers_count,
        --friends_count,
        id,
        BOOLEAN2CHAR(is_translator),
        listed_count,
        location,
        name,
        BOOLEAN2CHAR(protected),
        screen_name,
        statuses_count,
        url,
        BOOLEAN2CHAR(verified),
        BOOLEAN2CHAR(withheld)
    from tuser_load
    '''

    def __init__(self, filename, connection, fast=False):
        '''
        Constructor
        if fast is True, it does not guarantee to select the last register for the user (not such a big deal anyway, pending of ddbb optimization)
        '''
        Abstract_load.__init__(self, connection)
        self.filename = filename
        self.fast = fast
        
    def recreate_external_table(self):
        # change file name in definition
        self.external_table_definition_query = User_load.external_table_definition_preformatted.format(
            external_table_filename = self.filename
        )
        #print(q)
        Abstract_load.recreate_external_table_abstract(self)
        
    def insert_into_target(self):
        # avoid duplicates: disable pk
        Abstract_load.generic_query(self, "alter table tuser disable constraint tuser_pk")
        # insert-select
        Abstract_load.insert_into_target(self, do_commit=False)
        # remove duplicates
        if (self.fast == True):
            Abstract_load.generic_query(self, """
                delete from tuser
                where rowid not in (select max(rowid) from tuser group by id)
            """, do_commit=True)
        else:
            Abstract_load.generic_query(self, """
                delete from tuser
                where rowid not in (select max(rowid) keep (DENSE_RANK first order by statuses_count desc) from tuser group by id)
            """, do_commit=True)
        # enable pk
        Abstract_load.generic_query(self, "alter table tuser enable constraint tuser_pk")
        
            
        
        
        