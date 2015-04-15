package com.sisifo.rest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashSet;

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
		
		String recoverS = System.getProperty("recover");
		boolean recover = false; 
		if (recoverS != null) {
			recover = Boolean.valueOf(recoverS);
		}
				
		String suffix = "_from_list";
		if (recover) {
			suffix += System.getProperty("suffix");
		}
		
		FriendsFileWriter fw = new FriendsFileWriter(suffix);
		FavoriteFileWriter favw = new FavoriteFileWriter(suffix);
		UserInfoThread thread = new UserInfoThread();
		thread.startup(fw, favw, consumerKey, consumerSecret);
		thread.start();

		int totalUsers = 0;
	    String line;
	    boolean skip = false;
	    Long lastUserId = null;
	    if (recover) {
	    	skip = true;
    		try (BufferedReader brLastUser = new BufferedReader(new FileReader(UserInfoThread.LAST_USER_FILE_NAME))) {
    		    while ((line = brLastUser.readLine()) != null) {
    		    	lastUserId = Long.valueOf(line);
    		    	break;
    		    }
    		}
	    }

	    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    while ((line = br.readLine()) != null) {
		    	if (skip) {
		    		Long userId = Long.valueOf(line);
		    		if (userId.equals(lastUserId)) {
		    			skip = false;
		    		}
		    	} else {
		    		thread.addUserId(Long.valueOf(line));
		    		totalUsers++;
		    	}
		    }
		}
		
		while (thread.isAlive()) {
			Thread.sleep(1000);
			if (UserInfoThread.Status.IDDLE.equals(thread.getStatus())) {
				thread.interrupt();
				break;
			}
			if (UserInfoThread.Status.EXCEPTION.equals(thread.getStatus())) {
				LinkedHashSet<Long> remainingUsers = thread.getRemainingUsers();
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
