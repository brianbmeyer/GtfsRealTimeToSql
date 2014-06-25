package com.transitfeeds.gtfsrealtimetosql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FeedRunnerThread extends Thread {
    private String mConnectionStr, mUsername, mPassword;
	private long mInterval;
	private List<GtfsRealTimeFeed> mFeeds = new ArrayList<GtfsRealTimeFeed>();

	public FeedRunnerThread(String connectionStr, String username, String password, long intervalMs) {
	    mConnectionStr = connectionStr;
	    mUsername = username;
	    mPassword = password;
		mInterval = intervalMs;
	}

	public void addFeed(GtfsRealTimeFeed feed) {
		mFeeds.add(feed);
	}

	@Override
	public void run() {
	    Connection connection;
        GtfsRealTimeSqlRecorder recorder;
        
		try {
	        if (mConnectionStr.startsWith("jdbc:sqlite:")) {
	            Class.forName("org.sqlite.JDBC");
	        }
	        else if (mConnectionStr.startsWith("jdbc:postgresql:")) {
	            Class.forName("org.postgresql.Driver");
	        }
	        
	        connection = DriverManager.getConnection(mConnectionStr, mUsername, mPassword);
	        recorder = new GtfsRealTimeSqlRecorder(connection);
			recorder.startup();
		} catch (Exception e) {
            e.printStackTrace();
			return;
		}

		while (true) {
			try {
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

				recorder.commit();

				System.err.println(String.format("Sleeping %dms", mInterval));

				Thread.sleep(mInterval);
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
