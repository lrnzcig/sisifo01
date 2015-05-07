package twittertest;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.sisifo.twitter_model.utils.FriendsFileWriter;
import com.sisifo.twitter_rest_client.exceptions.SisifoHttpErrorException;
import com.sisifo.twitter_rest_client.utils.SearchTweetsUtils;
import com.sisifo.twitter_rest_client.utils.TokenUtils;
import com.sisifo.twitter_rest_client.utils.TwitterToken;
import com.sisifo.twitter_rest_client.utils.UserInfoUtils;

public class searchTweets {

	@Test
	public void getTweets() {
		String consumerKey = System.getProperty("consumerKey");
		String consumerSecret = System.getProperty("consumerSecret");
		
		TwitterToken token = TokenUtils.obtainToken(consumerKey, consumerSecret);
		
		String query = "@lrnzcig";
		// minimum date
		Calendar cal = new GregorianCalendar();
		cal.set(2015, Calendar.MAY, 1, 0, 0, 0);
		Date minDate = cal.getTime();
		cal.set(2015, Calendar.MAY, 8, 0, 0, 0);
		Date maxDate = cal.getTime();
		
		
		try {
			Set<Long> users = SearchTweetsUtils.writeTweetsToFile(query, token.getAccess_token(), minDate, maxDate,
					consumerKey, consumerSecret);
			System.out.println("Writing friends (" + users.size() + ")....");
			FriendsFileWriter w = new FriendsFileWriter(query);
			int i = 1;
			for (Long userId : users) {
				UserInfoUtils.writeFriendsToFile(userId, token.getAccess_token(), w,
						consumerKey, consumerSecret);
				if (i++ % 100 == 0) {
					System.out.println(i + " friends written");					
				}
			}
			System.out.println("Ended writing friends!");
			w.close();
		} catch (SisifoHttpErrorException e) {
			Assert.fail("Connection error " + e.getMessage());
		} catch (InterruptedException e) {
			Assert.fail("Interrupted exception " + e.getMessage());
		}
  	  	
		return;
		
	}
}
