package com.sisifo.twitter_rest_client.utils;

import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.client.Client;
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
	// assuming that the max number of access will occur in a window less than 15 min...
	private static final int SEARCH_TWEETS_MAX_NUMBER_OF_REQUESTS = 450;
	
	/**
	 * Uses api.twitter.com/1.1/search/tweets.json
	 * 
	 * @param query
	 * @param token
	 * @param minDate bellow this date, no tweets are retrieved (there could be a maximum of SEARCH_TWEETS_MAX_COUNT)
	 * @param consumerKey 
	 * @param consumerSecret 
	 * @return userIds in a Set
	 * @throws SisifoHttpErrorException
	 * @throws InterruptedException 
	 */
	public static Set<Long> writeTweetsToFile(String query, String token, Date minDate, String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		Client client = ClientUtils.getClientWithAuthenticationAndJackson();
		TweetFileWriter w = new TweetFileWriter(query);
		Set<Long> outputUserIds = new HashSet<>();
		
		// 1st access, no max_id
		Tweet lastTweet = getTweets(query, token, client, w, outputUserIds, null, consumerKey, consumerSecret);
		
		boolean lastRequestPending = true;
		for (int i = 1; (i <= SEARCH_TWEETS_MAX_NUMBER_OF_REQUESTS || minDate != null); i++) {		
			// check min Date
			if (minDate != null) {
				try {
					Date dateOfLastTweet = CommonTwitterUtils.dateFormat.parse(lastTweet.getCreated_at());
					if (minDate.after(dateOfLastTweet)) {
						System.out.println("Min date reached. Date of last tweet: " + lastTweet.getCreated_at());
						System.out.println("Request number " + i + " for query " + query);
						System.out.println("Number of users: " + outputUserIds.size());				
						lastRequestPending = false;
						break;
					}
				} catch (ParseException e) {
					// should not happen
					throw new RuntimeException(e);
				}
			}
			
			if (i % 100 == 0 || i == 1) {
				System.out.println("Request number " + i + " for query " + query);
				System.out.println("Number of users: " + outputUserIds.size());				
			}

			lastTweet = getTweets(query, token, client, w, outputUserIds, lastTweet.getId(), consumerKey, consumerSecret);
		}
		
		if (lastRequestPending) {
			System.out.println("Final request (" + SEARCH_TWEETS_MAX_NUMBER_OF_REQUESTS + " for query " + query);
			System.out.println("Number of users: " + outputUserIds.size());
		}
		w.close();
		
		return outputUserIds;
	}

	private static Tweet getTweets(String query, String token, Client client, 
			TweetFileWriter w, Set<Long> outputUserIds, Long lastTweetId,
			String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		try {
			Response response;
			if (lastTweetId == null) {
				response = client.target(SEARCH_TWEETS_URL)
					.queryParam("q", query)
					.queryParam("count", SEARCH_TWEETS_MAX_COUNT)
					.queryParam("lang", CommonTwitterUtils.LANGUAGE)
					.queryParam("result_type", "recent")	// return the most recent id's in the result
					.request(MediaType.APPLICATION_JSON)
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
		            .get();
			} else {
				response = client.target(SEARCH_TWEETS_URL)
						.queryParam("q", query)
						.queryParam("count", SEARCH_TWEETS_MAX_COUNT)
						.queryParam("lang", CommonTwitterUtils.LANGUAGE)
						.queryParam("result_type", "recent")	// return the most recent id's in the result
						.queryParam("max_id", lastTweetId)
						.request(MediaType.APPLICATION_JSON)
						.header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
			            .get();			
			}
			ClientUtils.checkResponseStatus(response);
			// get output and lastId
			JsonTweetsRestApi searchTweetsOutput = response.readEntity(JsonTweetsRestApi.class);
			Tweet lastTweet = w.writeToFile(searchTweetsOutput.getStatuses(), true);
			outputUserIds.addAll(CommonTwitterUtils.getUserIds(searchTweetsOutput.getStatuses()));
			w.flush();
			return lastTweet;
		} catch (SisifoTooManyRequestsException e) {
			TwitterToken newToken = TokenUtils.obtainTokenAfterTooManyRequests(consumerKey, consumerSecret);
			return getTweets(query, newToken.getAccess_token(), client, w, outputUserIds, lastTweetId, consumerKey, consumerSecret);
		}
	}

}
