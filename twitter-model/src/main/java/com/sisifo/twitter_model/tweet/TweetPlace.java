package com.sisifo.twitter_model.tweet;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TweetPlace {

	private String full_name;

	public TweetPlace() {
		super();
	}

	public String getFull_name() {
		return full_name;
	}

	public void setFull_name(String full_name) {
		this.full_name = full_name;
	}
	
}
