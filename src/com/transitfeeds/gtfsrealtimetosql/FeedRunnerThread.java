package com.transitfeeds.gtfsrealtimetosql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FeedRunnerThread extends Thread {
	private GtfsRealTimeSqlRecorder mRecorder;
	private long mInterval;
	private List<GtfsRealTimeFeed> mFeeds = new ArrayList<GtfsRealTimeFeed>();

	public FeedRunnerThread(GtfsRealTimeSqlRecorder recorder, long intervalMs) {
		mRecorder = recorder;
		mInterval = intervalMs;
	}

	public void addFeed(GtfsRealTimeFeed feed) {
		mFeeds.add(feed);
	}

	@Override
	public void run() {
		try {
			mRecorder.startup();
		} catch (Exception e) {
			return;
		}

		while (true) {
			try {
				mRecorder.begin();

				for (GtfsRealTimeFeed feed : mFeeds) {
					try {
						feed.load();
					} catch (Exception e) {
						continue;
					}

					try {
						mRecorder.record(feed.getFeedMessage());
					} catch (Exception e) {

					}
				}

				mRecorder.commit();

				System.err.println(String.format("Sleeping %dms", mInterval));

				Thread.sleep(mInterval);
			} catch (SQLException se) {
				break;
			} catch (InterruptedException e) {
				break;
			}
		}

		try {
			mRecorder.shutdown();
		} catch (Exception e) {

		}
	}
}
