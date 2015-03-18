OPTIONS (SKIP=1)
LOAD DATA
CHARACTERSET UTF8
APPEND INTO TABLE TWEET
FIELDS TERMINATED BY ';'
(
	created_at TIMESTAMP WITH TIME ZONE "DY MON DD HH24:MI:SS TZHTZM YYYY" ENCLOSED BY '\'',
	favorite_count,
	id,
	in_reply_to_status_id "CLEANUP(:in_reply_to_status_id)",
	in_reply_to_user_id "CLEANUP(:in_reply_to_user_id)",
	place_full_name ENCLOSED BY '\'',
	retweet_count,
	retweeted ENCLOSED BY '\'' "BOOLEAN2CHAR(:retweeted)",
	retweeted_id ENCLOSED BY '\'',
	text ENCLOSED BY '\'',
	truncated ENCLOSED BY '\'' "BOOLEAN2CHAR(:truncated)",
	user_id INTEGER
)
