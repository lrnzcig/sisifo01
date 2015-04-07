package com.sisifo.twitter_rest_client.utils;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sisifo.twitter_model.tweet.JsonTweetsRestApi;
import com.sisifo.twitter_model.tweet.Tweet;
import com.sisifo.twitter_model.utils.TweetFileWriter;
import com.sisifo.twitter_rest_client.exceptions.SisifoHttpErrorException;
import com.sisifo.twitter_rest_client.exceptions.SisifoTooManyRequestsException;

public class SearchTweetsUtils {

	private static final String SEARCH_TWEETS_URL = "https://api.twitter.com/1.1/search/tweets.json";
	private static final String SEARCH_TWEETS_MAX_COUNT = "100";
	
	/**
	 * Uses api.twitter.com/1.1/search/tweets.json
	 * 
	 * @param query
	 * @param token
	 * @param minDate bellow this date, no tweets are retrieved (there could be a maximum of SEARCH_TWEETS_MAX_COUNT)
	 * @param maxDate for the query, only date, no time!!
	 * @param consumerKey 
	 * @param consumerSecret 
	 * @param until parameter for the query, date until tweets are fetched (only date, no time!!!)
	 * @return userIds in a Set
	 * @throws SisifoHttpErrorException
	 * @throws InterruptedException 
	 */
	public static Set<Long> writeTweetsToFile(String query, String token, Date minDate, Date maxDate, String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		Client client = ClientUtils.getClientWithAuthenticationAndJackson();
		TweetFileWriter w = new TweetFileWriter(query);
		Set<Long> outputUserIds = new HashSet<>();
		
		// 1st access, no max_id
		Tweet lastTweet = getTweets(query, token, client, w, outputUserIds, null, maxDate, consumerKey, consumerSecret);

		if (lastTweet == null) {
			System.out.println("No tweets for " + query);
			w.close();
			return null;
		}
		int count = 1;
		while (lastTweet != null) {		
			// check min Date
			if (minDate != null) {
				try {
					Date dateOfLastTweet = CommonTwitterUtils.dateFormat.parse(lastTweet.getCreated_at());
					if (minDate.after(dateOfLastTweet)) {
						System.out.println("Min date reached. Date of last tweet: " + lastTweet.getCreated_at());
						System.out.println("Request number " + count + " for query " + query);
						System.out.println("Number of users: " + outputUserIds.size());				
						break;
					}
				} catch (ParseException e) {
					// should not happen
					throw new RuntimeException(e);
				}
			}
			
			if (count % 100 == 0 || count == 1) {
				System.out.println("Request number " + count + " for query " + query);
				System.out.println("Number of users: " + outputUserIds.size());				
			}
			Tweet previousLastTweet = lastTweet;
			lastTweet = getTweets(query, token, client, w, outputUserIds, lastTweet.getId(), maxDate, consumerKey, consumerSecret);
			if (lastTweet == null) {
				System.out.println("Last request number " + count + " for query " + query);
				System.out.println("Number of users: " + outputUserIds.size());				
				break;
			}
			if (previousLastTweet.getId().equals(lastTweet.getId())) {
				// a tweet is filling up a page and causes an infinite loop !!!
				Date lastTweetCreatedDate;
				try {
					lastTweetCreatedDate = CommonTwitterUtils.dateFormat.parse(lastTweet.getCreated_at());
				} catch (ParseException e) {
					// not going to happen, date is coming from twitter
					throw new RuntimeException(e);
				}
				Calendar cal = new GregorianCalendar();
				cal.setTime(lastTweetCreatedDate);
				cal.add(Calendar.DATE, -1);
				lastTweet = getTweets(query, token, client, w, outputUserIds, null, cal.getTime(), consumerKey, consumerSecret);
				if (lastTweet == null) {
					System.out.println("Infinite loop... Take a look to the code!!!");
					throw new RuntimeException("Infinite loop... Take a look to the code!!!");
				}
			}
			count++;
		}
		
		w.close();
		
		return outputUserIds;
	}

	private static Tweet getTweets(String query, String token, Client client, 
			TweetFileWriter w, Set<Long> outputUserIds, Long lastTweetId, Date until,
			String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		try {
			WebTarget target = client.target(SEARCH_TWEETS_URL)
					.queryParam("q", query)
					.queryParam("count", SEARCH_TWEETS_MAX_COUNT)
					.queryParam("lang", CommonTwitterUtils.LANGUAGE)
					.queryParam("result_type", "recent");	// return the most recent id's in the result
			if (lastTweetId != null) {
				target = target.queryParam("max_id", lastTweetId);
			}
			if (until != null) {
				target = target.queryParam("until", CommonTwitterUtils.queryDateFormat.format(until));
			}
			Response response = target.request(MediaType.APPLICATION_JSON)
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
		            .get();
			ClientUtils.checkResponseStatus(response);
			// get output and lastId
			JsonTweetsRestApi searchTweetsOutput = response.readEntity(JsonTweetsRestApi.class);
			Tweet lastTweet = w.writeToFile(searchTweetsOutput.getStatuses(), true);
			outputUserIds.addAll(CommonTwitterUtils.getUserIds(searchTweetsOutput.getStatuses()));
			w.flush();
			return lastTweet;
		} catch (SisifoTooManyRequestsException e) {
			TwitterToken newToken = TokenUtils.obtainTokenAfterTooManyRequests(consumerKey, consumerSecret);
			return getTweets(query, newToken.getAccess_token(), client, w, outputUserIds, lastTweetId, until, consumerKey, consumerSecret);
		}
	}

}
