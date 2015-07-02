
package com.sisifo.jersey_server_test.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

/** Example resource class hosted at the URI path "/myresource"
 */
@Path("/myresource")
public class MyResource {
    
    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET 
    @Produces("text/plain")
    public String getIt(@QueryParam("queryParam") String param) {
    	if (param == null) {
    		return "null!";
    	}
    	if (!"value".equals(param)) {
    		throw new RuntimeException("falta ?queryParam=value");
    	}
        return "Hi there!";
    }
}
