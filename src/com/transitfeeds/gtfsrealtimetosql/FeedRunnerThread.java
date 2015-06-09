package com.transitfeeds.gtfsrealtimetosql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FeedRunnerThread extends Thread {
    private String mConnectionStr, mUsername, mPassword;
	private long mDefaultInterval, mCurrentInterval, mMaxInterval;
	private List<GtfsRealTimeFeed> mFeeds = new ArrayList<GtfsRealTimeFeed>();
	
	public FeedRunnerThread(String connectionStr, String username, String password, long intervalMs) {
	    mConnectionStr = connectionStr;
	    mUsername = username;
	    mPassword = password;
		mDefaultInterval = intervalMs;
		mMaxInterval = intervalMs * 5;
	}

	public void addFeed(GtfsRealTimeFeed feed) {
		mFeeds.add(feed);
	}

	@Override
	public void run() {
		try {
	        if (mConnectionStr.startsWith("jdbc:sqlite:")) {
	            Class.forName("org.sqlite.JDBC");
	        }
	        else if (mConnectionStr.startsWith("jdbc:postgresql:")) {
	            Class.forName("org.postgresql.Driver");
	        }
	        
		} catch (Exception e) {
            e.printStackTrace();
			return;
		}

        Connection connection = null;
        GtfsRealTimeSqlRecorder recorder = null;

        mCurrentInterval = mDefaultInterval;
        
        while (true) {
			try {
			    if (connection == null) {
			        System.err.println(String.format("Connecting to database: %s", mConnectionStr));
			        connection = DriverManager.getConnection(mConnectionStr, mUsername, mPassword);
			        recorder = new GtfsRealTimeSqlRecorder(connection);
			        recorder.startup();
			    }
			    
				recorder.begin();

				for (GtfsRealTimeFeed feed : mFeeds) {
					try {
						feed.load();
					} catch (Exception e) {
					    e.printStackTrace();
						continue;
					}

					try {
						recorder.record(feed.getFeedMessage());
					} catch (Exception e) {
                        e.printStackTrace();
					}
				}
				
				boolean reconnect = recorder.getNumOpenQueries() > 0;

				if (!reconnect) {
				    recorder.commit();
				}

				if (reconnect) {
				    System.err.println(String.format("Disconnecting from %s", mConnectionStr));
				    connection.close();
				    connection = null;
				    
				    mCurrentInterval += mDefaultInterval;
				}
				else {
				    mCurrentInterval = mDefaultInterval;
				}
				
				if (mCurrentInterval > mMaxInterval) {
                    System.err.println(String.format("Interval too large (%dms), exiting", mCurrentInterval));
				    break;
				}
				else {
                    System.err.println(String.format("Sleeping %dms", mCurrentInterval));
                    Thread.sleep(mCurrentInterval);
				}
			} catch (SQLException se) {
                se.printStackTrace();
				break;
			} catch (InterruptedException e) {
				break;
			}
		}

        try {
            recorder.shutdown();
        } catch (Exception e) {

        }

        try {
            connection.close();
        } catch (Exception e) {

        }
	}
}
