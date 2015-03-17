package com.sisifo.twitter_rest_client.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import com.sisifo.twitter_model.tweet.Tweet;

public class CommonTwitterUtils {

	public static final String LANGUAGE = "es";
	// Sun Mar 15 23:59:39 +0000 2015
	public static final DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);

	public static Collection<Long> getUserIds(Tweet[] statuses) {
		Set<Long> output = new HashSet<Long>();
		if (statuses == null) {
			return output;
		}
		for (Tweet tweet : statuses) {
			output.add(tweet.getUser_id());
		}
		return output;
	}
	

}
