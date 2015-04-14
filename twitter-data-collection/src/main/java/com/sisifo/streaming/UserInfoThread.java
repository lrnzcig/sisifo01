package com.sisifo.streaming;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;

import com.sisifo.twitter_model.utils.FavoriteFileWriter;
import com.sisifo.twitter_model.utils.FriendsFileWriter;
import com.sisifo.twitter_rest_client.exceptions.SisifoHttpErrorException;
import com.sisifo.twitter_rest_client.utils.TokenUtils;
import com.sisifo.twitter_rest_client.utils.TwitterToken;
import com.sisifo.twitter_rest_client.utils.UserInfoUtils;

public class UserInfoThread extends Thread {
	
	public static final String LAST_USER_FILE_NAME = "last_user.txt";
	
	private Set<Long> userIds = new HashSet<>();
	private Set<Long> processedUserIds = new HashSet<>();
	private Set<Long> errorUserIds = new HashSet<>();
	private FriendsFileWriter friew;
	private FavoriteFileWriter favw;
	private String consumerKey;
	private String consumerSecret;
	private Exception e = null;
	private boolean running;
	private int initialTotal = 0;
	
	public void startup(FriendsFileWriter fw, FavoriteFileWriter favw, String consumerKey, String consumerSecret) {
		this.friew = fw;
		this.favw = favw;
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.running = false;
	}
	
	public void setInitialTotal(int newTotal) {
		if (running) {
			System.out.println("Too late to setup initial total. Already started!");
		}
		this.initialTotal = newTotal;
	}

	@Override
	public void run() {
		running = true;
		// first token
		TwitterToken token = TokenUtils.obtainToken(consumerKey, consumerSecret);
		// until externally stopped
		int total = this.initialTotal;
		System.out.println("Thread started, looking for users' friends/favorites lists.");
		while (true) {
			Long userId = getNextUserToBeProcessed();
			if (userId != null) {
				BufferedWriter writer = null;
				try {
					UserInfoUtils.writeFriendsToFile(userId, token.getAccess_token(), friew, consumerKey, consumerSecret);
					UserInfoUtils.writeFavoritesToFile(userId, token.getAccess_token(), favw, consumerKey, consumerSecret);
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(LAST_USER_FILE_NAME)));
					writer.write(userId.toString());
				} catch (SisifoHttpErrorException e) {
					e.printStackTrace();
					this.e = e;
					System.out.println("(Exception) Wrote " + total + " users' friends/favorites lists." + " User id: " + userId + " (added to errors)");
					errorUserIds.add(userId);
					return;
				} catch (InterruptedException e) {
					e.printStackTrace();
					this.e = e;
					return;
				} catch (IOException e) {
					e.printStackTrace();
					this.e = e;
					return;
				} finally {
					try {writer.close();} catch (Exception ex) {}
				}
				processedUserIds.add(userId);
				if (++total % 50 == 0) {
					friew.flush();
					System.out.println("Wrote " + total + " users' friends/favorites lists.");
				}
			} else {
				// wait for more friends to be added to the wanted list
				try {
					System.out.println("Thread iddle.");
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					switch (getStatus()) {
					case EXCEPTION:
						throw new RuntimeException(this.e);
					case USERS_PENDING:
						throw new RuntimeException("There were still users to fetch!");
					default:
						System.out.println("Thread ended ok");
						break;
					}
				}
			}
		}
		
	}
	
	private Long getNextUserToBeProcessed() {
		for (Long userId : userIds) {
			if (! processedUserIds.contains(userId)) {
				return userId;
			}
		}
		return null;
	}

	synchronized public void addUserId(Long userId) {
		userIds.add(userId);
	}

	public Exception getException() {
		return e;
	}
	
	public enum Status {
		NOT_STARTED,
		IDDLE,
		USERS_PENDING,
		EXCEPTION
	}
	
	public Status getStatus() {
		if (! running) {
			return Status.NOT_STARTED;
		}
		if (e != null) {
			return Status.EXCEPTION;
		}
		if (getNextUserToBeProcessed() == null) {
			return Status.IDDLE;
		}
		return Status.USERS_PENDING;
	}

	public Set<Long> getRemainingUsers() {
		Set<Long> output = new HashSet<>();
		for (Long userId : userIds) {
			if ((! processedUserIds.contains(userId)) 
					&& (! errorUserIds.contains(userId))) {
				output.add(userId);
			}
		}
		return output;
	}

	public void setUsers(Set<Long> users) {
		userIds = users;
		System.out.println(users.size() + " users to fetch");
	}


}
