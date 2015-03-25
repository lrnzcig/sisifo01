package com.sisifo.twitter_rest_client.exceptions;

import javax.ws.rs.core.Response;

@SuppressWarnings("serial")
public class SisifoTooManyRequestsException extends Exception {

	public SisifoTooManyRequestsException(Response response) {
		super(response.getStatusInfo().getReasonPhrase());
	}

}
