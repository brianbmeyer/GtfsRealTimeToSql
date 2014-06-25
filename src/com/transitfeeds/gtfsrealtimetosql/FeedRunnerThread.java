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
	        connection = DriverManager.getConnection(mConnectionStr, mUsername, mPassword);
	        recorder = new GtfsRealTimeSqlRecorder(connection);
			recorder.startup();
		} catch (Exception e) {
			return;
		}

		while (true) {
			try {
				recorder.begin();

				for (GtfsRealTimeFeed feed : mFeeds) {
					try {
						feed.load();
					} catch (Exception e) {
						continue;
					}

					try {
						recorder.record(feed.getFeedMessage());
					} catch (Exception e) {

					}
				}

				recorder.commit();

				System.err.println(String.format("Sleeping %dms", mInterval));

				Thread.sleep(mInterval);
			} catch (SQLException se) {
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
