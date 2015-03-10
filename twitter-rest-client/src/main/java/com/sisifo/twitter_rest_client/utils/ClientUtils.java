package com.sisifo.twitter_rest_client.utils;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;


public class ClientUtils {

	public static Client getClientWithSslContext() throws NoSuchAlgorithmException, KeyManagementException {
		HostnameVerifier hostnameVerifier = ClientUtils.getDefaultHostnameVerifier();
        System.setProperty("jsse.enableSNIExtension", "false");
        TrustManager[] certs = ClientUtils.getDefaultTrustManager(); 
        
		SSLContext ctx = SSLContext.getInstance("SSL");
		ctx.init(null, certs, new SecureRandom());
		Client client = ClientBuilder.newBuilder().sslContext(ctx).hostnameVerifier(hostnameVerifier).build();
		
		HttpAuthenticationFeature feature = HttpAuthenticationFeature.basicBuilder().build();
		client.register(feature);
		return client;
	}	
	
	private static HostnameVerifier getDefaultHostnameVerifier() {
		return new HostnameVerifier() {
			
			@Override
			public boolean verify(String hostname, SSLSession session) {
				return true;
			}
		};
	}

	private static TrustManager[] getDefaultTrustManager() {
		return 	new TrustManager[] {
				new javax.net.ssl.X509TrustManager()
	            	{
						@Override
						public void checkClientTrusted(X509Certificate[] chain,
								String authType) throws CertificateException {
						}

						@Override
						public void checkServerTrusted(X509Certificate[] chain,
								String authType) throws CertificateException {
						}

						@Override
						public X509Certificate[] getAcceptedIssuers() {
							return null;
						}
	            	}
	        	};
	}

}
