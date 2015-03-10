package com.sisifo.jersey_server_test;

import javax.ws.rs.Priorities;

import org.glassfish.jersey.server.ResourceConfig;



public class MyApplication extends ResourceConfig {

	public MyApplication() {
		super();
		
		register(AuthenticationRequestFilter.class, Priorities.AUTHENTICATION);
		
		packages("com.sisifo.jersey_server_test.resources");
	}


}
