package com.sisifo.twitter_rest_client.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sisifo.twitter_model.tweet.JsonFriendsRestApi;
import com.sisifo.twitter_model.utils.FriendsFileWriter;

public class GetFriendsUtils {

	private static final String GET_FRIENDS_MAX_COUNT = "5000";

	public static void writeFriendsToFile(Long userId, String token, FriendsFileWriter w) {
		w.writeToFile(userId, getFriends(userId, token));
		w.flush();
	}

	public static Set<Long> getFriends(Long userId, String token) {
		Set<Long> friends = new HashSet<Long>();

		Client client = ClientUtils.getClientWithAuthenticationAndJackson();
		// 1st access, no cursor
		Response response = client.target("https://api.twitter.com/1.1/friends/ids.json")
				.queryParam("user_id", userId)
				//.queryParam("screen_name", "@lrnzcig")
				.queryParam("count", GET_FRIENDS_MAX_COUNT)
				.request(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
	            .get();
		
		Long cursor = processResponse(response, friends);
		
		while (cursor != 0) {
			response = client.target("https://api.twitter.com/1.1/friends/ids.json")
					.queryParam("user_id", userId)
					.queryParam("count", GET_FRIENDS_MAX_COUNT)
					.queryParam("cursor", cursor)
					.request(MediaType.APPLICATION_JSON)
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
		            .get();
			cursor = processResponse(response, friends);
		}
		
		return friends;

	}

	private static Long processResponse(Response response, Set<Long> friends) {
		JsonFriendsRestApi json = response.readEntity(JsonFriendsRestApi.class);
		if (json.getIds() == null) {
			return 0L;
		}
		friends.addAll(Arrays.asList(json.getIds()));
		return json.getNext_cursor();
		
	}
}
