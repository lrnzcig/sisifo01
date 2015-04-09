package com.sisifo.rest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

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

		int totalUsers = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	thread.addUserId(Long.valueOf(line));
		    	totalUsers++;
		    }
		}
		
		while (thread.isAlive()) {
			Thread.sleep(1000);
			if (UserInfoThread.Status.IDDLE.equals(thread.getStatus())) {
				thread.interrupt();
				break;
			}
			if (UserInfoThread.Status.EXCEPTION.equals(thread.getStatus())) {
				Set<Long> remainingUsers = thread.getRemainingUsers();
				thread.interrupt();
				thread = new UserInfoThread();
				thread.startup(fw, favw, consumerKey, consumerSecret);
				thread.setInitialTotal(totalUsers - remainingUsers.size());
				thread.start();
				thread.setUsers(remainingUsers);
			}
		}
		Thread.sleep(1000);


	}
}
