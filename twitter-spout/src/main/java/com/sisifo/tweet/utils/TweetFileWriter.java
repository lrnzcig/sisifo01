package com.sisifo.tweet.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import com.sisifo.tweet.JsonTweetEntity;
import com.sisifo.tweet.JsonTweetHashtag;
import com.sisifo.tweet.JsonTweetUrl;
import com.sisifo.tweet.JsonTweetUserMention;
import com.sisifo.tweet.JsonUserMentions;
import com.sisifo.tweet.Tweet;
import com.sisifo.tweet.TweetUser;

public class TweetFileWriter {


	private CSVFormat csvFormat;
	
	private static final Object[] TWEET_HEADER = {"created_at",
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
	private FileWriter tweetFileWriter;
	private CSVPrinter tweetCsvPrinter;

	private static final Object[] TWEET_USER_HEADER = {"contributors_enabled",
		"created_at",
		"description",
		"favourites_count",
		"followers_count",
		"friends_count",
		"id",
		"is_translator",
		"listed_count",
		"location",
		"name",
		"protected",
		"screen_name",
		"statuses_count",
		"url",
		"verified",
		"withheld"
	};
	private FileWriter tweetUserFileWriter;
	private CSVPrinter tweetUserCsvPrinter;
	
	private static final Object[] TWEET_HASHTAG_HEADER = {"tweet_id",
		"hashtag"
	};
	private FileWriter tweetHashtagFileWriter;
	private CSVPrinter tweetHashtagCsvPrinter;

	private static final Object[] TWEET_URL_HEADER = {"tweet_id",
		"url"
	};
	private FileWriter tweetUrlFileWriter;
	private CSVPrinter tweetUrlCsvPrinter;

	private static final Object[] TWEET_USER_MENTION_HEADER = {"tweet_id",
		"source_user_id",
		"target_user_id"
	};
	private FileWriter tweetUserMentionFileWriter;
	private CSVPrinter tweetUserMentionCsvPrinter;

	private static final Object[] TWEET_USER_URL_HEADER = {"tweet_id",
		"url"
	};
	private FileWriter tweetUserUrlFileWriter;
	private CSVPrinter tweetUserUrlCsvPrinter;

	public TweetFileWriter() {
		super();
		csvFormat = CSVFormat.DEFAULT.withDelimiter(';').withQuote('\'').withQuoteMode(QuoteMode.NON_NUMERIC);
		try {
			tweetFileWriter = new FileWriter("tweets.csv");
	    	tweetCsvPrinter = new CSVPrinter(tweetFileWriter, csvFormat);
	    	tweetCsvPrinter.printRecord(TWEET_HEADER);
	    	
	    	tweetUserFileWriter = new FileWriter("tweetUsers.csv");
	    	tweetUserCsvPrinter = new CSVPrinter(tweetUserFileWriter, csvFormat);
	    	tweetUserCsvPrinter.printRecord(TWEET_USER_HEADER);

	    	tweetHashtagFileWriter = new FileWriter("tweetHashtags.csv");
	    	tweetHashtagCsvPrinter = new CSVPrinter(tweetHashtagFileWriter, csvFormat);
	    	tweetHashtagCsvPrinter.printRecord(TWEET_HASHTAG_HEADER);

	    	tweetUrlFileWriter = new FileWriter("tweetUrls.csv");
	    	tweetUrlCsvPrinter = new CSVPrinter(tweetUrlFileWriter, csvFormat);
	    	tweetUrlCsvPrinter.printRecord(TWEET_URL_HEADER);

	    	tweetUserMentionFileWriter = new FileWriter("tweetUserMentions.csv");
	    	tweetUserMentionCsvPrinter = new CSVPrinter(tweetUserMentionFileWriter, csvFormat);
	    	tweetUserMentionCsvPrinter.printRecord(TWEET_USER_MENTION_HEADER);

	    	tweetUserUrlFileWriter = new FileWriter("tweetUserUrls.csv");
	    	tweetUserUrlCsvPrinter = new CSVPrinter(tweetUserUrlFileWriter, csvFormat);
	    	tweetUserUrlCsvPrinter.printRecord(TWEET_USER_URL_HEADER);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private List<List<Object>> getTweetHashtagRecords(Tweet tweet) {
		JsonTweetEntity entities = tweet.getEntities();
		if (entities == null 
				|| entities.getHashtags() == null
				|| entities.getHashtags().length == 0) {
			return null;
		}
		List<List<Object>> output = new ArrayList<List<Object>>();
		for (JsonTweetHashtag hashtag : entities.getHashtags()) {
			List<Object> record = new ArrayList<Object>();
			record.add(tweet.getId());
			record.add(hashtag.getText());
			output.add(record);
		}
		return output;
	}

	private List<List<Object>> getTweetUrlRecords(Tweet tweet) {
		JsonTweetEntity entities = tweet.getEntities();
		if (entities == null 
				|| entities.getUrls() == null
				|| entities.getUrls().length == 0) {
			return null;
		}
		List<List<Object>> output = new ArrayList<List<Object>>();
		for (JsonTweetUrl url : entities.getUrls()) {
			List<Object> record = new ArrayList<Object>();
			record.add(tweet.getId());
			record.add(url.getUrl());
			output.add(record);
		}
		return output;
	}

	private List<List<Object>> getTweetUserMentionRecords(Tweet tweet) {
		JsonTweetEntity entities = tweet.getEntities();
		if (entities == null 
				|| entities.getUser_mentions() == null
				|| entities.getUser_mentions().length == 0) {
			return null;
		}
		List<List<Object>> output = new ArrayList<List<Object>>();
		for (JsonTweetUserMention userMention : entities.getUser_mentions()) {
			List<Object> record = new ArrayList<Object>();
			record.add(tweet.getId());
			record.add(tweet.getUser_id());
			record.add(userMention.getId());
			output.add(record);
		}
		return output;
	}

	private List<List<Object>> getUserUrlRecords(Tweet tweet) {
		JsonTweetEntity entities = tweet.getUser().getEntities();
		if (entities == null 
				|| entities.getUrls() == null
				|| entities.getUrls().length == 0) {
			return null;
		}
		List<List<Object>> output = new ArrayList<List<Object>>();
		for (JsonTweetUrl url : entities.getUrls()) {
			List<Object> record = new ArrayList<Object>();
			record.add(tweet.getId());
			record.add(url.getUrl());
			output.add(record);
		}
		return output;
	}

	private List<Object> getTweetUserRecord(Tweet tweet) {
		TweetUser user = tweet.getUser();
		List<Object> record = new ArrayList<Object>();
		record.add(user.isContributors_enabled());
		record.add(user.getCreated_at());
		record.add(user.getDescription());
		record.add(user.getFavourites_count());
		record.add(user.getFollowers_count());
		record.add(user.getId());
		record.add(user.isIs_translator());
		record.add(user.getListed_count());
		record.add(user.getLocation());
		record.add(user.getName());
		record.add(user.isProtectedFlag());
		record.add(user.getScreen_name());
		record.add(user.getStatuses_count());
		record.add(user.getUrl());
		record.add(user.isVerified());
		record.add(user.isWithheld());
		return record;
	}

	public void writeToFile(Tweet tweet) {
		try {
			List<Object> recordTweet = getTweetRecord(tweet);
			tweetCsvPrinter.printRecord(recordTweet);
			List<Object> recordTweetUser = getTweetUserRecord(tweet);
			tweetUserCsvPrinter.printRecord(recordTweetUser);
			List<List<Object>> hashtags = getTweetHashtagRecords(tweet);
			if (hashtags != null) {
				for (List<Object> hashtag : hashtags) {
					tweetHashtagCsvPrinter.printRecord(hashtag);
				}
			}
			List<List<Object>> urls = getTweetUrlRecords(tweet);
			if (urls != null) {
				for (List<Object> url : urls) {
					tweetUrlCsvPrinter.printRecord(url);
				}
			}
			List<List<Object>> userMentions = getTweetUserMentionRecords(tweet);
			if (userMentions != null) {
				for (List<Object> userMention : userMentions) {
					tweetUserMentionCsvPrinter.printRecord(userMention);
				}
			}
			List<List<Object>> userUrls = getUserUrlRecords(tweet);
			if (userUrls != null) {
				for (List<Object> userUrl : userUrls) {
					tweetUserUrlCsvPrinter.printRecord(userUrl);
				}
			}
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	private List<Object> getTweetRecord(Tweet tweet) {
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
		record.add(tweet.getText());
		record.add(tweet.getTruncated());
		record.add(tweet.getUser_id());
		return record;
	}

	public void close() {
        try {
        	tweetFileWriter.flush();
        	tweetFileWriter.close();
            tweetCsvPrinter.close();
            tweetUserFileWriter.flush();
            tweetUserFileWriter.close();
            tweetUserCsvPrinter.close();
            tweetHashtagFileWriter.flush();
            tweetHashtagFileWriter.close();
            tweetHashtagCsvPrinter.close();
            tweetUrlFileWriter.flush();
            tweetUrlFileWriter.close();
            tweetUrlCsvPrinter.close();
            tweetUserMentionFileWriter.flush();
            tweetUserMentionFileWriter.close();
            tweetUserMentionCsvPrinter.close();
            tweetUserUrlFileWriter.flush();
            tweetUserUrlFileWriter.close();
            tweetUserUrlCsvPrinter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	

}
