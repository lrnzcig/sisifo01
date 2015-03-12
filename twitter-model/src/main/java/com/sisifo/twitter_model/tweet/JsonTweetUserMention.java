package com.sisifo.twitter_model.tweet;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class JsonTweetUserMention {

	private Long id;

	public JsonTweetUserMention() {
		super();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}
	
}
