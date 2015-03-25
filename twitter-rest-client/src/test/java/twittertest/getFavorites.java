package twittertest;

import org.junit.Test;

import com.sisifo.twitter_model.utils.FavoriteFileWriter;
import com.sisifo.twitter_rest_client.exceptions.SisifoHttpErrorException;
import com.sisifo.twitter_rest_client.utils.TokenUtils;
import com.sisifo.twitter_rest_client.utils.TwitterToken;
import com.sisifo.twitter_rest_client.utils.UserInfoUtils;

public class getFavorites {

	@Test
	public void get() throws SisifoHttpErrorException, InterruptedException {
		String consumerKey = System.getProperty("consumerKey");
		String consumerSecret = System.getProperty("consumerSecret");
		
		TwitterToken token = TokenUtils.obtainToken(consumerKey, consumerSecret);

		Long userId = 2969945374L;
		FavoriteFileWriter w = new FavoriteFileWriter();
		UserInfoUtils.writeFavoritesToFile(userId, token.getAccess_token(), w, consumerKey, consumerSecret);
		w.close();
	}
}
