package com.sisifo.twitter_rest_client.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sisifo.twitter_model.tweet.JsonFriendsRestApi;
import com.sisifo.twitter_model.tweet.Tweet;
import com.sisifo.twitter_model.utils.FavoriteFileWriter;
import com.sisifo.twitter_model.utils.FriendsFileWriter;
import com.sisifo.twitter_rest_client.exceptions.SisifoHttpErrorException;
import com.sisifo.twitter_rest_client.exceptions.SisifoTooManyRequestsException;

/**
 * Methods for:
 * - getting friends of a user
 * - getting list of favorite tweets of a user
 * 
 * @author lorenzorubio
 *
 */
public class UserInfoUtils {

	private static final String GET_FRIENDS_URL = "https://api.twitter.com/1.1/friends/ids.json";
	private static final String GET_FRIENDS_MAX_COUNT = "5000";
	
	private static final String GET_FAVORITES_URL = "https://api.twitter.com/1.1/favorites/list.json";
	private static final String GET_FAVORITES_MAX_COUNT = "200";

	public static Set<Long> writeFriendsToFile(Long userId, String currentToken, FriendsFileWriter w,
			String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		Set<Long> friends = getFriends(userId, currentToken, consumerKey, consumerSecret);
		w.writeToFile(userId, friends);
		w.flush();
		return friends;
	}

	public static void writeFavoritesToFile(Long userId, String currentToken, FavoriteFileWriter w,
			String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		Tweet[] favTweets = getFavoriteTweets(userId, currentToken, consumerKey, consumerSecret);
		w.writeToFile(userId, favTweets);
		w.flush();
		return;
	}

	private static Tweet[] getFavoriteTweets(Long userId, String currentToken,
			String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {

		Client client = ClientUtils.getClientWithAuthenticationAndJackson();
		Response response = client.target(GET_FAVORITES_URL)
				//.queryParam("user_id", userId)
				.queryParam("screen_name", "@lrnzcig")
				.queryParam("count", GET_FAVORITES_MAX_COUNT)
				.request(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + currentToken)
	            .get();

		Tweet[] tweets;
		try {
			tweets = processFavoritesResponse(response);
		} catch (SisifoTooManyRequestsException e) {
			TwitterToken newToken = TokenUtils.obtainTokenAfterTooManyRequests(consumerKey, consumerSecret);
			return getFavoriteTweets(userId, newToken.getAccess_token(), consumerKey, consumerSecret);
		}
		return tweets;
	}

	public static Set<Long> getFriends(Long userId, String currentToken,
			String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		Set<Long> friends = new HashSet<Long>();

		Client client = ClientUtils.getClientWithAuthenticationAndJackson();
		// 1st access, no cursor
		Response response = client.target(GET_FRIENDS_URL)
				.queryParam("user_id", userId)
				//.queryParam("screen_name", "@lrnzcig")
				.queryParam("count", GET_FRIENDS_MAX_COUNT)
				.request(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + currentToken)
	            .get();
		
		Long cursor;
		try {
			cursor = processFriendsResponse(response, friends);
		} catch (SisifoTooManyRequestsException e) {
			TwitterToken newToken = TokenUtils.obtainTokenAfterTooManyRequests(consumerKey, consumerSecret);
			return getFriends(userId, newToken.getAccess_token(), consumerKey, consumerSecret);
		}
		
		while (cursor != 0) {
			cursor = getFriendsFromCursor(client, userId, currentToken, friends, cursor, consumerKey, consumerSecret);
		}
		
		return friends;

	}
	
	private static Long getFriendsFromCursor(Client client, Long userId, String token, Set<Long> friends, Long initialCursor,
			String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		Response response = client.target(GET_FRIENDS_URL)
				.queryParam("user_id", userId)
				.queryParam("count", GET_FRIENDS_MAX_COUNT)
				.queryParam("cursor", initialCursor)
				.request(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
	            .get();
		try {
			return processFriendsResponse(response, friends);
		} catch (SisifoTooManyRequestsException e) {
			TwitterToken newToken = TokenUtils.obtainTokenAfterTooManyRequests(consumerKey, consumerSecret);
			return getFriendsFromCursor(client, userId, newToken.getAccess_token(), friends, initialCursor, consumerKey, consumerSecret);
		}
		
	}

	private static Long processFriendsResponse(Response response, Set<Long> friends) throws SisifoHttpErrorException, SisifoTooManyRequestsException {
		ClientUtils.checkResponseStatus(response);
		JsonFriendsRestApi json = response.readEntity(JsonFriendsRestApi.class);
		if (json.getIds() == null) {
			return 0L;
		}
		friends.addAll(Arrays.asList(json.getIds()));
		return json.getNext_cursor();		
	}
	
	private static Tweet[] processFavoritesResponse(Response response) throws SisifoHttpErrorException, SisifoTooManyRequestsException {
		ClientUtils.checkResponseStatus(response);
		return response.readEntity(Tweet[].class);
	}

}
