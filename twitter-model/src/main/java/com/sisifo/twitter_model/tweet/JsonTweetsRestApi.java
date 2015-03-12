package com.sisifo.twitter_model.tweet;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * List of tweets as in the REST API
 * 
 * @author lorenzorubio
 *
 */
@XmlRootElement
public class JsonTweetsRestApi {

	private Tweet[] statuses;
	
	public JsonTweetsRestApi() {
		super();
	}

	public Tweet[] getStatuses() {
		return statuses;
	}

	public void setStatuses(Tweet[] statuses) {
		this.statuses = statuses;
	}
	
}
