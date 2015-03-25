package com.sisifo.streaming;

import java.util.HashSet;
import java.util.Set;

import com.sisifo.twitter_model.utils.FriendsFileWriter;
import com.sisifo.twitter_rest_client.exceptions.SisifoHttpErrorException;
import com.sisifo.twitter_rest_client.utils.GetFriendsUtils;
import com.sisifo.twitter_rest_client.utils.TokenUtils;
import com.sisifo.twitter_rest_client.utils.TwitterToken;

public class GetFriendsThread extends Thread {
	
	private Set<Long> userIds = new HashSet<>();
	private Set<Long> processedUserIds = new HashSet<>();
	private FriendsFileWriter fw;
	private String consumerKey;
	private String consumerSecret;
	private Exception e = null;
	private boolean running;
	
	public void startup(FriendsFileWriter fw, String consumerKey, String consumerSecret) {
		this.fw = fw;
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.running = false;
	}

	@Override
	public void run() {
		running = true;
		// first token
		TwitterToken token = TokenUtils.obtainToken(consumerKey, consumerSecret);
		// until externally stopped
		int total = 0;
		while (true) {
			Long userId = getNextUserToBeProcessed();
			if (userId != null) {
				try {
					GetFriendsUtils.writeFriendsToFile(userId, token.getAccess_token(), fw, consumerKey, consumerSecret);
				} catch (SisifoHttpErrorException e) {
					e.printStackTrace();
					this.e = e;
					return;
				} catch (InterruptedException e) {
					e.printStackTrace();
					this.e = e;
					return;
				}
				processedUserIds.add(userId);
				if (++total % 100 == 0) {
					fw.flush();
					System.out.println("Wrote " + total + " users' friends lists.");
				}
			} else {
				// wait for more friends to be added to the wanted list
				try {
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


}
