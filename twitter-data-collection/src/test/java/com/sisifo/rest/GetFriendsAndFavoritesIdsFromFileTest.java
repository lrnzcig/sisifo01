package com.sisifo.rest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import com.sisifo.streaming.UserInfoThread;
import com.sisifo.twitter_model.utils.FavoriteFileWriter;
import com.sisifo.twitter_model.utils.FriendsFileWriter;

public class GetFriendsAndFavoritesIdsFromFileTest {

	@Test
	public void getSomeFriends() throws InterruptedException, FileNotFoundException, IOException {
		String consumerKey = System.getProperty("consumerKey");
		String consumerSecret = System.getProperty("consumerSecret");
		String fileName = System.getProperty("fileName");
				

		FriendsFileWriter fw = new FriendsFileWriter("_from_list");
		FavoriteFileWriter favw = new FavoriteFileWriter("_from_list");
		UserInfoThread thread = new UserInfoThread();
		thread.startup(fw, favw, consumerKey, consumerSecret);
		thread.start();

		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	thread.addUserId(Long.valueOf(line));
		    }
		}
		
		while (thread.isAlive()) {
			Thread.sleep(1000);
			if (UserInfoThread.Status.IDDLE.equals(thread.getStatus())) {
				thread.interrupt();
				break;
			}
		}
		Thread.sleep(1000);


	}
}
