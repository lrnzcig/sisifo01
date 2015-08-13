OPTIONS (SKIP=1)
LOAD DATA
CHARACTERSET UTF8
APPEND INTO TABLE TUSER
FIELDS TERMINATED BY ';'
(
	contributors_enabled ENCLOSED BY '\'' "BOOLEAN2CHAR(:contributors_enabled)",
	created_at TIMESTAMP WITH TIME ZONE "DY MON DD HH24:MI:SS TZHTZM YYYY" ENCLOSED BY '\'',
	description ENCLOSED BY '\'',
	favourites_count,
	followers_count,
	friends_count,
	id,
	is_translator ENCLOSED BY '\'' "BOOLEAN2CHAR(:is_translator)",
	listed_count,
	location ENCLOSED BY '\'',
	name ENCLOSED BY '\'',
	protected ENCLOSED BY '\'' "BOOLEAN2CHAR(:protected)",
	screen_name ENCLOSED BY '\'',
	statuses_count,
	url ENCLOSED BY '\'',
	verified ENCLOSED BY '\'' "BOOLEAN2CHAR(:verified)",
	withheld ENCLOSED BY '\'' "BOOLEAN2CHAR(:withheld)"
)
