OPTIONS (SKIP=1)
LOAD DATA
CHARACTERSET UTF8
APPEND INTO TABLE THASHTAG
FIELDS TERMINATED BY ';'
(
	tweet_id,
	hashtag ENCLOSED BY '\''
)