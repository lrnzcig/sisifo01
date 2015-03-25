package com.sisifo.rest;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.sisifo.twitter_model.utils.FriendsFileWriter;
import com.sisifo.twitter_rest_client.exceptions.SisifoHttpErrorException;
import com.sisifo.twitter_rest_client.utils.UserInfoUtils;
import com.sisifo.twitter_rest_client.utils.SearchTweetsUtils;
import com.sisifo.twitter_rest_client.utils.TokenUtils;
import com.sisifo.twitter_rest_client.utils.TwitterToken;

public class SearchTweetsRestApi {

	/**
	 * Search tweets up to minimum for each of the queries in the list
	 * 
	 * Example of minimum date:
	 * 		Calendar cal = new GregorianCalendar();
	 *		cal.set(2015, Calendar.MARCH, 16, 0, 0, 0);
	 *		Date minDate = cal.getTime();
	 * 
	 * @param queries e.g. "@ahorapodemos"
	 * @param minDate
	 * @param doFetchAdditionalUserInfo if active, get additional user info from REST API - this is very slow due to rate restrictions, around 15 queries every 15 minutes
	 * @throws InterruptedException
	 */
	public static void run(Collection<String> queries, Date minDate, boolean doFetchAdditionalUserInfo) throws InterruptedException {

		String consumerKey = System.getProperty("consumerKey");
		String consumerSecret = System.getProperty("consumerSecret");
				
		Map<Long, Set<Long>> allUsers = new HashMap<>();

		for (String query : queries) {
			try {
				// obtain new token per query
				TwitterToken token = TokenUtils.obtainToken(consumerKey, consumerSecret);

				System.out.println("Search tweets for query " + query + "....");
				Set<Long> users = SearchTweetsUtils.writeTweetsToFile(query, token.getAccess_token(), minDate, consumerKey, consumerSecret);
				
				if (doFetchAdditionalUserInfo) {
					fetchUserFriendList(users, allUsers, query, token, consumerKey, consumerSecret);
				}
				
			} catch (SisifoHttpErrorException e) {
				e.printStackTrace();
				throw new InterruptedException(e.getMessage());
			}
		}
		
	}

	private static void fetchUserFriendList(Set<Long> users, Map<Long, Set<Long>> allUsers, String query,
			TwitterToken token, String consumerKey, String consumerSecret) throws SisifoHttpErrorException, InterruptedException {
		System.out.println("Writing users' friends lists (" + users.size() + ")....");
		FriendsFileWriter w = new FriendsFileWriter(query);
		int total = 1;
		int friendsSkipped = 0;
		for (Long userId : users) {
			if (allUsers.get(userId) != null) {
				// user has already been fetched, assumed not enough changes for another access
				w.writeToFile(userId, allUsers.get(userId));
				w.flush();
				friendsSkipped++;
				continue;
			}
			Set<Long> friends = UserInfoUtils.writeFriendsToFile(userId, token.getAccess_token(), w, consumerKey, consumerSecret);
			allUsers.put(userId, friends);
			if (total++ % 100 == 0) {
				System.out.println(total + " users' friends lists written (" + friendsSkipped + " skipped)");					
			}
		}
		System.out.println("Ended writing users' friends lists! (query=" + query + ")");
		w.close();	// this is for the query only
	}
}
