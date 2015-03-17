package com.sisifo.twitter_rest_client.exceptions;

import javax.ws.rs.core.Response;

@SuppressWarnings("serial")
public class SisifoHttpErrorException extends Exception {

	public SisifoHttpErrorException(Response response) {
		super(response.getStatusInfo().getReasonPhrase());
	}

}
