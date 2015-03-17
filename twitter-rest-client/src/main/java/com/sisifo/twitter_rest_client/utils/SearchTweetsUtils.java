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

public class SearchTweetsUtils {

	
	private static final String SEARCH_TWEETS_MAX_COUNT = "100";
	// assuming that the max number of access will occur in a window less than 15 min...
	private static final int SEARCH_TWEETS_MAX_NUMBER_OF_REQUESTS = 450;
	
	/**
	 * Uses api.twitter.com/1.1/search/tweets.json
	 * 
	 * @param query
	 * @param token
	 * @param userIds already fetched in previous calls
	 * @param minDate bellow this date, no tweets are retrieved (there could be a maximum of SEARCH_TWEETS_MAX_COUNT)
	 * @return userIds already fetched + new ones in a Set
	 * @throws SisifoHttpErrorException
	 */
	public static Set<Long> writeTweetsToFile(String query, String token, Set<Long> userIds, Date minDate) throws SisifoHttpErrorException {
		Client client = ClientUtils.getClientWithAuthenticationAndJackson();
		TweetFileWriter w = new TweetFileWriter(query);
		Set<Long> outputUserIds = new HashSet<>();
		
		
		// 1st access, no max_id
		Response response = client.target("https://api.twitter.com/1.1/search/tweets.json")
				.queryParam("q", query)
				.queryParam("count", SEARCH_TWEETS_MAX_COUNT)
				.queryParam("lang", CommonTwitterUtils.LANGUAGE)
				.queryParam("result_type", "recent")	// return the most recent id's in the result
				.request(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
	            .get();

		
		boolean lastRequestPending = true;
		for (int i = 1; i <= SEARCH_TWEETS_MAX_NUMBER_OF_REQUESTS; i++) {
			ClientUtils.checkResponseStatus(response);
			
			// get output and lastId
			JsonTweetsRestApi searchTweetsOutput = response.readEntity(JsonTweetsRestApi.class);
			Tweet lastTweet = w.writeToFile(searchTweetsOutput.getStatuses(), true);
			outputUserIds.addAll(CommonTwitterUtils.getUserIds(searchTweetsOutput.getStatuses()));
			w.flush();
			
			// check min Date
			if (minDate != null) {
				try {
					Date dateOfLastTweet = CommonTwitterUtils.dateFormat.parse(lastTweet.getCreated_at());
					if (minDate.after(dateOfLastTweet)) {
						System.out.println("Min date reached. Date of last tweet: " + lastTweet.getCreated_at());
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

			response = client.target("https://api.twitter.com/1.1/search/tweets.json")
					.queryParam("q", query)
					.queryParam("count", SEARCH_TWEETS_MAX_COUNT)
					.queryParam("lang", CommonTwitterUtils.LANGUAGE)
					.queryParam("result_type", "recent")	// return the most recent id's in the result
					.queryParam("max_id", lastTweet.getId())
					.request(MediaType.APPLICATION_JSON)
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
		            .get();
		}
		
		if (lastRequestPending) {
			ClientUtils.checkResponseStatus(response);
			JsonTweetsRestApi searchTweetsOutput = response.readEntity(JsonTweetsRestApi.class);
			w.writeToFile(searchTweetsOutput.getStatuses(), true);
			w.close();
			System.out.println("Final request (" + SEARCH_TWEETS_MAX_NUMBER_OF_REQUESTS + " for query " + query);
			System.out.println("Number of users: " + outputUserIds.size());
		}
		
		// add users from the original list to the output list
		if (userIds != null) {
			outputUserIds.addAll(userIds);
		}
		return outputUserIds;
	}

}
