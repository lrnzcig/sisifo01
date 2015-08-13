create table tweet (
  created_at DATE,
  favorite_count INTEGER,
  id INTEGER not null,
  in_reply_to_status_id	INTEGER,
  in_reply_to_user_id	INTEGER,
  place_full_name VARCHAR(256),
  retweet_count	INTEGER,
  retweet	SMALLINT,
  retweeted_id INTEGER,
  retweeted_user_id INTEGER,
  text VARCHAR(256),
  truncated	CHAR(1),
  user_id	INTEGER not null,
  CONSTRAINT tweet_pk PRIMARY KEY (id)
);

create table tuser (
  contributors_enabled CHAR(1),
  created_at DATE,
  description	VARCHAR(1024),
  favourites_count INTEGER,
  followers_count INTEGER,
  friends_count	INTEGER,
  id INTEGER not null,
  is_translator	CHAR(1),
  listed_count INTEGER,
  location VARCHAR(256),
  name VARCHAR(256),
  protected	CHAR(1),
  screen_name	VARCHAR(256),
  statuses_count INTEGER,
  url	VARCHAR(1024),
  verified CHAR(1),
  withheld CHAR(1),
  CONSTRAINT tuser_pk PRIMARY KEY (id)
);

create table thashtag (
  tweet_id INTEGER not null,
  hashtag VARCHAR(256) not null,
  CONSTRAINT thashtag_pk PRIMARY KEY (tweet_id, hashtag)
);

create table turl (
  tweet_id INTEGER not null,
  url VARCHAR(1024) not null,
  CONSTRAINT turl_pk PRIMARY KEY (tweet_id, url)
);

create table tusermention (
  tweet_id INTEGER not null,
  source_user_id INTEGER not null,
  target_user_id INTEGER not null,
  CONSTRAINT tusermention_pk PRIMARY KEY (tweet_id, source_user_id, target_user_id)
);

create table tuserurl (
  user_id INTEGER not null,
  url VARCHAR(1024),
  CONSTRAINT tuserurl_pk PRIMARY KEY (user_id, url)
);

create table follower (
  user_id INTEGER not null,
  followed_user_id INTEGER not null,
  start_date DATE,
  end_date DATE,
  CONSTRAINT follower_pk PRIMARY KEY (user_id, followed_user_id)
);

CREATE OR REPLACE FUNCTION BOOLEAN2CHAR(inp VARCHAR2) RETURN VARCHAR2 AS 
BEGIN
  IF (inp = 'true') THEN
    RETURN '1';
  END IF;
  IF (inp = 'false') THEN
    RETURN '0';
  END IF;
  RETURN NULL;
END BOOLEAN2CHAR;

CREATE OR REPLACE FUNCTION CLEANUP(inp VARCHAR2) RETURN VARCHAR2 AS 
BEGIN
  RETURN regexp_replace(inp, '[^09]', '');
END CLEANUP;

select * from tweet;
select * from thashtag;
select * from turl;
select * from tuser;
select * from tusermention;


select BOOLEAN2CHAR('true') from dual;
select CLEANUP('a') from dual;
select regexp_replace('0', '[^09]', '') from dual;
-- drop table tweet;
-- drop table tuser;
-- delete from tweet;
-- delete from thashtag;
-- delete from tusermention;
-- delete from follower;
-- delete from tuser;