package com.sisifo.twitter_model.utils;

import java.util.ArrayList;
import java.util.List;

import com.sisifo.twitter_model.tweet.Tweet;

public class TweetWriterUtils {

	public static final Object[] TWEET_HEADER = {"created_at",
		"favorite_count",
		"id",
		"in_reply_to_status_id",
		"in_reply_to_user_id",
		"place_full_name",
		"retweet_count",
		"retweeted",
		"retweeted_id",
		"text",
		"truncated",
		"user_id"};

	public static List<Object> getTweetRecord(Tweet tweet) {
		List<Object> record = new ArrayList<Object>();
		record.add(tweet.getCreated_at());
		record.add(tweet.getFavorite_count());
		record.add(tweet.getId());
		record.add(tweet.getIn_reply_to_status_id());
		record.add(tweet.getIn_reply_to_user_id());
		record.add(tweet.getPlace_full_name());
		record.add(tweet.getRetweet_count());
		record.add(tweet.getRetweeted());
		record.add(tweet.getRetweeted_id());
		record.add(tweet.getTextCleanedUp());
		record.add(tweet.getTruncated());
		record.add(tweet.getUser_id());
		return record;
	}


}
