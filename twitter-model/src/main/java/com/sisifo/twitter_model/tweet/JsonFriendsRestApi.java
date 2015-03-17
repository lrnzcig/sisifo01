package com.sisifo.twitter_model.tweet;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * List of friends as in the REST API
 * 
 * @author lorenzorubio
 *
 */
@XmlRootElement
public class JsonFriendsRestApi {

	private Long[] ids;
	private Long next_cursor;
	
	public JsonFriendsRestApi() {
		super();
	}

	public Long[] getIds() {
		return ids;
	}

	public void setIds(Long[] ids) {
		this.ids = ids;
	}

	public Long getNext_cursor() {
		return next_cursor;
	}

	public void setNext_cursor(Long next_cursor) {
		this.next_cursor = next_cursor;
	}

	
	
}
