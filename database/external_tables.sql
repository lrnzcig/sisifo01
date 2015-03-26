-- create directory load_dir as '/home/oracle/Desktop/restapi1';

create table tweet_load (
  created_at varchar(256),
  favorite_count VARCHAR(256),
  id VARCHAR(256),
  in_reply_to_status_id	VARCHAR(256),
  in_reply_to_user_id	VARCHAR(256),
  place_full_name VARCHAR(256),
  retweet_count	VARCHAR(256),
  retweeted	VARCHAR(256),
  retweeted_id	VARCHAR(256),
  text VARCHAR(256),
  truncated	VARCHAR(256),
  user_id	VARCHAR(256)
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
   location ('trial.csv')
  );
  
-- select * from tweet_load
-- drop table tweet_load


-- insert into tweet
select cleanup(created_at)
from tweet_load

select to_timestamp(created_at, 'DY MON DD HH24:MI:SS TZHTZM YYYY')
from tweet_load