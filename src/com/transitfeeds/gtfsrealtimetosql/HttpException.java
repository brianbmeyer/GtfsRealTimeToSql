package com.transitfeeds.gtfsrealtimetosql;

public class HttpException extends Exception {

	private static final long serialVersionUID = 7930601825832893952L;
	
	private int mHttpCode;

	public HttpException(String message, int httpCode) {
		super(message);
		
		mHttpCode = httpCode;
	}

	public int getHttpCode() {
		return mHttpCode;
	}

}
