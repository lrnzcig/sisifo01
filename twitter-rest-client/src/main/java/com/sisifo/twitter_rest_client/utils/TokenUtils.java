package com.sisifo.twitter_rest_client.utils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TokenUtils {

	public static TwitterToken obtainToken(String consumerKey,
			String consumerSecret) {
		/**
		 * Twitter application-only authentication
		 * 
		 * https://dev.twitter.com/oauth/application-only
		 */

		// Step 1. Encode consumer key and secret
		// AND
		// Step 2. Obtain a bearer token
		Client client = ClientUtils.getClientWithAuthenticationAndJackson();
	    Response postResponse = client.target("https://api.twitter.com/oauth2/token")
	            .request(MediaType.APPLICATION_JSON)
	            .property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_USERNAME, consumerKey)
				.property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_PASSWORD, consumerSecret)
	            .post(Entity.entity("grant_type=client_credentials",  MediaType.APPLICATION_FORM_URLENCODED));

	    // Step 3. Authenticate API requests with the bearer token
	    String tokenOutput = postResponse.readEntity(String.class);
	    // {"token_type":"bearer","access_token":"AAAAAAAAAAAAAAAAAAAAA..."}
	    GsonBuilder builder = new GsonBuilder();
	    Gson gson = builder.create();
  	  	return gson.fromJson(tokenOutput, TwitterToken.class);
	}

	public static TwitterToken obtainTokenAfterTooManyRequests(
			String consumerKey, String consumerSecret) throws InterruptedException {
		System.out.println("REST API: too many requests. Waiting for 15 minutes (streaming will continue)");
		Thread.sleep(900000);
		return obtainToken(consumerKey, consumerSecret);
	}

}
