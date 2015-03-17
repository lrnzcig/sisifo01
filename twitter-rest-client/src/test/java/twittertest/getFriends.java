package twittertest;

import org.junit.Test;

import com.sisifo.twitter_model.utils.FriendsFileWriter;
import com.sisifo.twitter_rest_client.utils.GetFriendsUtils;
import com.sisifo.twitter_rest_client.utils.TokenUtils;
import com.sisifo.twitter_rest_client.utils.TwitterToken;

public class getFriends {

	@Test
	public void get() {
		String consumerKey = System.getProperty("consumerKey");
		String consumerSecret = System.getProperty("consumerSecret");
		
		TwitterToken token = TokenUtils.obtainToken(consumerKey, consumerSecret);

		Long userId = 2969945374L;
		FriendsFileWriter w = new FriendsFileWriter();
		GetFriendsUtils.writeFriendsToFile(userId, token.getAccess_token(), w);
		w.close();
	}
}
