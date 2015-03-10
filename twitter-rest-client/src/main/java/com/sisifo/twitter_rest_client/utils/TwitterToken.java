package com.sisifo.twitter_rest_client.utils;

public class TwitterToken {

	private String token_type;
	private String access_token;
	
	public TwitterToken() {
		super();
	}

	public String getToken_type() {
		return token_type;
	}

	public void setToken_type(String token_type) {
		this.token_type = token_type;
	}

	public String getAccess_token() {
		return access_token;
	}

	public void setAccess_token(String access_token) {
		this.access_token = access_token;
	}
	
	
}
