'''
Created on 30/3/2015

@author: lorenzorubio
'''
from load_database.external_tables.abstract_load import Abstract_load

class Tweet_load(Abstract_load):
    '''
    manages external table tweet_load
    '''
    
    external_table_drop_query = '''
        drop table tweet_load
    '''
    external_table_definition_preformatted = '''
        create table tweet_load (
            id VARCHAR(256),
            created_at varchar(256),
            favorite_count VARCHAR(256),
            in_reply_to_status_id    VARCHAR(256),
            in_reply_to_user_id    VARCHAR(256),
            retweet_count    VARCHAR(256),
            truncated    VARCHAR(256),
            user_id    VARCHAR(256),
            retweet    VARCHAR(256),
            retweeted_id    VARCHAR(256),
            retweeted_user_id    VARCHAR(256),
            text VARCHAR(256)
            )
        organization external
            (type oracle_loader
             default directory load_dir
             access parameters
             (records delimited by newline skip 1
              characterset utf8
              badfile '{external_table_filename}.bad'
              logfile '{external_table_filename}.log'
              FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '\"'
              LRTRIM
             )
            location ('{external_table_filename}')
            )
        reject limit unlimited
    '''
    insert_select_query = '''
        insert into tweet
            (id,
            created_at,
            favorite_count,
            in_reply_to_status_id,
            in_reply_to_user_id,
            retweet_count,
            text,
            truncated,
            user_id,
            retweet,
            retweeted_id,
            retweeted_user_id)
        select 
            id,
            to_timestamp_tz(created_at, 'DY MON DD HH24:MI:SS TZHTZM YYYY', 'NLS_DATE_LANGUAGE = AMERICAN') AT time zone 'CET',
            favorite_count,
            in_reply_to_status_id,
            in_reply_to_user_id,
            retweet_count,
            text,
            BOOLEAN2CHAR(truncated),
            user_id,
            retweet,
            retweeted_id,
            retweeted_user_id
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
        self.external_table_definition_query = self.external_table_definition_preformatted.format(
            external_table_filename = self.filename
        )
        #print(q)
        self.recreate_external_table_abstract()

    def insert_into_target(self):
        # avoid duplicates: disable pk
        self.generic_query("alter table tweet disable constraint tweet_pk", do_commit=False)
        # insert-select
        Abstract_load.insert_into_target(self, do_commit=False)
        # remove duplicates
        self.generic_query("""
                delete from tweet
                where rowid in (select rowid from tweet
                                minus select max(rowid) keep (DENSE_RANK first order by retweet_count desc) from tweet group by id)
            """, do_commit=True)
        # enable pk
        self.generic_query("alter table tusermention enable constraint tusermention_pk", do_commit=False)                
        
        
        