package com.sisifo.tweet;

import java.lang.reflect.Type;

import com.google.gson.InstanceCreator;

public class TweetInstanceCreator implements InstanceCreator<Tweet> {

	@Override
	public Tweet createInstance(Type arg0) {
		return new Tweet();
	}

}
