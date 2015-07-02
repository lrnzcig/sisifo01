package servertest;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.Assert;
import org.junit.Test;

import com.sisifo.twitter_rest_client.utils.ClientUtils;


public class GetMyResourceTest {

	@Test
	public void getTest() throws NoSuchAlgorithmException, KeyManagementException {	
		Client client = ClientUtils.getClientWithSslContext();
		
		Response response = client.target("http://localhost:8080/jersey-server-test/webresources").path("myresource").queryParam("queryParam", "value").request()
		//Response response = client.target("http://localhost:8080/jersey-server-test/webresources").path("myresource").request()
				.property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_USERNAME, "kk")
				.property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_PASSWORD, "pass")
				.get();
		Assert.assertEquals(200, response.getStatus());
		System.out.println(response.readEntity(String.class));
		return;
	}
}
