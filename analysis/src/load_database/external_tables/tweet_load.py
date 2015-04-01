'''
Created on 30/3/2015

@author: lorenzorubio
'''
from abstract_load import Abstract_load

class Tweet_load(Abstract_load):
    '''
    manages external table tweet_load
    '''
    
    external_table_drop_query = '''
        drop table tweet_load
    '''
    external_table_definition_preformatted = '''
        create table tweet_load (
            created_at varchar(256),
            favorite_count VARCHAR(256),
            id VARCHAR(256),
            in_reply_to_status_id    VARCHAR(256),
            in_reply_to_user_id    VARCHAR(256),
            place_full_name VARCHAR(256),
            retweet_count    VARCHAR(256),
            retweeted    VARCHAR(256),
            retweeted_id    VARCHAR(256),
            text VARCHAR(256),
            truncated    VARCHAR(256),
            user_id    VARCHAR(256)
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
        insert into tweet
        select 
            to_timestamp_tz(created_at, 'DY MON DD HH24:MI:SS TZHTZM YYYY', 'NLS_DATE_LANGUAGE = AMERICAN') AT time zone 'CET',
            favorite_count,
            id,
            in_reply_to_status_id,
            in_reply_to_user_id,
            place_full_name,
            retweet_count,
            BOOLEAN2CHAR(retweeted),
            retweeted_id,
            text,
            BOOLEAN2CHAR(truncated),
            user_id
        from tweet_load
    '''

    def __init__(self, filename, connection):
        '''
        Constructor
        '''
        Abstract_load.__init__(self, connection)
        self.filename = filename
        
    def recreate_external_table(self):
        # change file name in definition
        self.external_table_definition_query = Tweet_load.external_table_definition_preformatted.format(
            external_table_filename = self.filename
        )
        #print(q)
        Abstract_load.recreate_external_table_abstract(self)
        
    def insert_into_target(self):
        Abstract_load.insert_into_target(self)
            
        
        
        