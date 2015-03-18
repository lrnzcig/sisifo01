create table TWEET (
  created_at	TIMESTAMP,
  favorite_count	INTEGER,
  id INTEGER not null,
  in_reply_to_status_id	INTEGER,
  in_reply_to_user_id	INTEGER,
  place_full_name VARCHAR(256),
  retweet_count	INTEGER,
  retweeted	CHAR(1),
  retweeted_id	INTEGER,
  text VARCHAR(256),
  truncated	CHAR(1),
  user_id	INTEGER not null
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

select BOOLEAN2CHAR('true') from dual;
select CLEANUP('a') from dual;
select regexp_replace('0', '[^09]', '') from dual;
-- drop table tweet;