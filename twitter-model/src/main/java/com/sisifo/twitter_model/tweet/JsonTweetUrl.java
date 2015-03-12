package com.sisifo.twitter_model.tweet;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class JsonTweetUrl {

	private String url;

	public JsonTweetUrl() {
		super();
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
}
