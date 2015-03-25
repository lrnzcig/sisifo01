package com.sisifo.rest;

import org.junit.Test;

import com.sisifo.streaming.UserInfoThread;
import com.sisifo.twitter_model.utils.FavoriteFileWriter;
import com.sisifo.twitter_model.utils.FriendsFileWriter;

public class GetFriendsThreadTest {

	@Test
	public void getSomeFriends() throws InterruptedException {
		String consumerKey = System.getProperty("consumerKey");
		String consumerSecret = System.getProperty("consumerSecret");
				

		FriendsFileWriter fw = new FriendsFileWriter("streaming");
		FavoriteFileWriter favw = new FavoriteFileWriter("streaming");
		UserInfoThread thread = new UserInfoThread();
		thread.startup(fw, favw, consumerKey, consumerSecret);
		thread.start();

		thread.addUserId((long) 1716506203);
		thread.addUserId((long) 271918106);
		
		boolean firstTime = true;
		while (thread.isAlive()) {
			Thread.sleep(1000);
			if (UserInfoThread.Status.IDDLE.equals(thread.getStatus())) {
				if (firstTime) {
					// add a user which has been already processed, do nothing
					thread.addUserId((long) 1716506203);
					firstTime = false;
				} else {
					thread.interrupt();
					break;
				}
			}
		}
		Thread.sleep(1000);


	}
}
