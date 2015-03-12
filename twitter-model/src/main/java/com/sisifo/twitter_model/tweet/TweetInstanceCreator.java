package com.sisifo.twitter_model.tweet;

import java.lang.reflect.Type;

import com.google.gson.InstanceCreator;

public class TweetInstanceCreator implements InstanceCreator<Tweet> {

	public Tweet createInstance(Type arg0) {
		return new Tweet();
	}

}
