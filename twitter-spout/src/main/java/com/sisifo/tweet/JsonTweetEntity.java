package com.sisifo.tweet;

public class JsonTweetEntity {

	private JsonTweetHashtag[] hashtags;
	private JsonTweetUrl[] urls;
	private JsonTweetUserMention[] user_mentions;
	
	public JsonTweetEntity() {
		super();
	}

	public JsonTweetHashtag[] getHashtags() {
		return hashtags;
	}

	public void setHashtags(JsonTweetHashtag[] hashtags) {
		this.hashtags = hashtags;
	}

	public JsonTweetUrl[] getUrls() {
		return urls;
	}

	public void setUrls(JsonTweetUrl[] urls) {
		this.urls = urls;
	}

	public JsonTweetUserMention[] getUser_mentions() {
		return user_mentions;
	}

	public void setUser_mentions(JsonTweetUserMention[] user_mentions) {
		this.user_mentions = user_mentions;
	}

	
	
}
