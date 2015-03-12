package com.sisifo.twitter_model.tweet;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class JsonTweetHashtag {

	private String text;
	
	public JsonTweetHashtag() {
		super();
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	
}
