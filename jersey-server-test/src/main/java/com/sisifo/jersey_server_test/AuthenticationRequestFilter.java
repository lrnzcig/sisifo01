package com.sisifo.jersey_server_test;

import java.io.IOException;
import java.security.Principal;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.jersey.internal.util.Base64;

@PreMatching
public class AuthenticationRequestFilter implements ContainerRequestFilter {
	
    @Override
    public void filter(final ContainerRequestContext requestContext) throws IOException {
        String authentication = requestContext.getHeaderString(HttpHeaders.AUTHORIZATION);
        if (! authentication.startsWith("Basic ")) {
            throw new RuntimeException("Not authenticated");
        }
        authentication = authentication.substring("Basic ".length());
        String[] values = Base64.decodeAsString(authentication).split(":");
        if (values.length < 2) {
            throw new WebApplicationException("Invalid syntax for username and password");
        }
        
        final String username = values[0];
        String password = values[1];
        if ((username == null) || (password == null)) {
            throw new WebApplicationException("Missing username or password");
        }
        
        requestContext.setSecurityContext(new SecurityContext() {
			
			@Override
			public boolean isUserInRole(String arg0) {
				return false;
			}
			
			@Override
			public boolean isSecure() {
				return true;
			}
			
			@Override
			public Principal getUserPrincipal() {
				return new Principal() {
					
					@Override
					public String getName() {
						return username;
					}
				};
			}
			
			@Override
			public String getAuthenticationScheme() {
				return "Basic";
			}
		});

    }

}
