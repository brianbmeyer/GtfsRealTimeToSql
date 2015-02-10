package com.transitfeeds.gtfsrealtimetosql;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.sun.org.apache.xml.internal.security.utils.Base64;

public class GtfsRealTimeFeed {

	private String mUrl;
	private String mUsername;
	private String mPassword;
	private FeedMessage mFeedMessage;

	public GtfsRealTimeFeed(String url) {
		mUrl = url;
	}
	
	public void setCredentials(String username, String password) {
		mUsername = username;
		mPassword = password;
	}

	public FeedMessage getFeedMessage()
	{
		return mFeedMessage;
	}
	
	private static final String HTTPS = "https";
	private static final String GZIP = "gzip";
	
	public void load() throws Exception {
		String url = mUrl;
		
		HttpClient httpClient;

		System.err.println("Loading " + url + " ...");
		
		URI uri = new URI(url);

		HttpClientBuilder builder = HttpClientBuilder.create();

		if (url.startsWith(HTTPS)) {
			SSLContext sslContext = SSLContext.getInstance("SSL");
			
			sslContext.init(null, new TrustManager[] { new javax.net.ssl.X509TrustManager() {
				
				@Override
				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}
				
				@Override
				public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
				}
				
				@Override
				public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
				}
			} }, new SecureRandom());
			
			int port = uri.getPort();
			
			if (port < 0) {
				port = 443;
			}

			builder.setSslcontext(sslContext);
		}

		httpClient = builder.build();
		
		HttpGet httpGet = new HttpGet(uri);
		httpGet.setHeader("Accept-Encoding", GZIP);
		httpGet.setHeader("Accept", "application/x-protobuf");

		String username = mUsername;
		String password = mPassword;

		if (username != null && password != null) {
			String creds = String.format("%s:%s", username, password);
			httpGet.setHeader("Authorization", "Basic " + Base64.encode(creds.getBytes()));
		}

		HttpResponse response = httpClient.execute(httpGet);

		if (response.getStatusLine().getStatusCode() != 200) {
			throw new HttpException("Unexpected response: " + response.getStatusLine().toString(), response.getStatusLine().getStatusCode());
		}

		HttpEntity httpEntity = response.getEntity();
		
		Header contentEncoding = httpEntity.getContentEncoding();

		InputStream is;
		
		if (contentEncoding == null || !contentEncoding.getValue().equalsIgnoreCase(GZIP)) {
			is = httpEntity.getContent();
		}
		else {
			is = new GZIPInputStream(new ByteArrayInputStream(EntityUtils.toByteArray(httpEntity)));
		}

		mFeedMessage = GtfsRealtime.FeedMessage.parseFrom(is);
		System.err.println("Finished Loading " + url);
	}
}
