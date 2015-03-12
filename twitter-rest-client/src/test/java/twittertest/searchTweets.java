package twittertest;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sisifo.twitter_model.tweet.JsonTweetsRestApi;
import com.sisifo.twitter_model.utils.TweetFileWriter;
import com.sisifo.twitter_rest_client.utils.ClientUtils;
import com.sisifo.twitter_rest_client.utils.TwitterToken;

public class searchTweets {

	@Test
	public void getTweets() throws KeyManagementException, NoSuchAlgorithmException {
		/**
		 * Twitter application-only authentication
		 * 
		 * https://dev.twitter.com/oauth/application-only
		 */
		String consumerKey = System.getProperty("consumerKey");
		String consumerSecret = System.getProperty("consumerSecret");
		// Step 1. Encode consumer key and secret
		// AND
		// Step 2. Obtain a bearer token
		Client client = ClientUtils.getClientWithAuthentication();
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
  	  	TwitterToken token = gson.fromJson(tokenOutput, TwitterToken.class);
  	  	
		Response response = client.target("https://api.twitter.com/1.1/search/tweets.json")
				.queryParam("q", "@psebastianbueno")
				.queryParam("count", "100")
				.request(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + token.getAccess_token())
	            .get();

		Assert.assertEquals(200, response.getStatus());
		JsonTweetsRestApi searchTweetsOutput = response.readEntity(JsonTweetsRestApi.class);
		TweetFileWriter w = new TweetFileWriter();
		w.writeToFile(searchTweetsOutput.getStatuses());
		return;
		
	}
}
