-- drop directory load_dir;
-- create directory load_dir as '/media/sf_sisifo01/restapi1'; -- does not work!
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
   location ('tweets_ahorapodemos.csv.cr')
  )
reject limit unlimited;
  
-- select * from tweet_load
-- drop table tweet_load


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
--where id in (
--select distinct(id) from tweet_load);



-- friends_count not retrieved in all data files
-- friends_count	VARCHAR(256),
create table tuser_load (
  contributors_enabled VARCHAR(256),
  created_at VARCHAR(256),
  description	VARCHAR(1024),
  favourites_count VARCHAR(256),
  followers_count VARCHAR(256),
  id VARCHAR(256),
  is_translator	VARCHAR(256),
  listed_count VARCHAR(256),
  location VARCHAR(256),
  name VARCHAR(256),
  protected	VARCHAR(256),
  screen_name	VARCHAR(256),
  statuses_count VARCHAR(256),
  url	VARCHAR(1024),
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
   location ('tweetUsers_ahorapodemos.csv.cr')
  )
reject limit unlimited;
-- drop table tuser_load
-- select * from tuser_load

alter table tuser disable constraint tuser_pk;

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
--where tuser_load.rowid in (select max(rowid) from tuser_load group by id)
--where id in (select distinct(id) from tuser_load)

delete from tuser
where rowid not in (select max(rowid) keep (DENSE_RANK first order by statuses_count) from tuser group by id);

alter table tuser enable constraint tuser_pk;

-- delete from tuser;

insert into tuser
(contributors_enabled, created_at, description, 
  -- favourites_count, followers_count, 
  -- friends_count,
  id, is_translator, 
  -- listed_count,
  location, name, protected, screen_name, 
  -- statuses_count,
  url, verified, withheld)
select distinct 
  BOOLEAN2CHAR(contributors_enabled),
  to_timestamp_tz(created_at, 'DY MON DD HH24:MI:SS TZHTZM YYYY', 'NLS_DATE_LANGUAGE = AMERICAN') AT time zone 'CET',
  description,
  --favourites_count,
  --followers_count,
  --friends_count,
  id,
  BOOLEAN2CHAR(is_translator),
  --listed_count,
  location,
  name,
  BOOLEAN2CHAR(protected),
  screen_name,
  --statuses_count,
  url,
  BOOLEAN2CHAR(verified),
  BOOLEAN2CHAR(withheld)
from tuser_load

-- delete from tuser

select distinct * from tuser_load
where id = '193796539'
and statuses_count = select 
group by id

select id, count(*) from tuser_load
group by id
having count(*) > 1
order by id
---------------------

select id, count(*)
from tweet_load
group by id
having count(*) > 1

select id from tweet_load
where id = '?577742286613794816';

?select id from tweet_load
where id = '?577742345162104832';

order by id;

select * from tweet;
-- delete from tweet
-- delete from tuser
