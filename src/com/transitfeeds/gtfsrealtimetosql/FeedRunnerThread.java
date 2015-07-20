package com.transitfeeds.gtfsrealtimetosql;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FeedRunnerThread extends Thread {
    private String mConnectionStr, mUsername, mPassword;
	private long mDefaultInterval, mCurrentInterval, mMaxInterval;
	private List<GtfsRealTimeFeed> mFeeds = new ArrayList<GtfsRealTimeFeed>();
	
	private Logger mLogger;

	private List<Handler> mHandlers = new ArrayList<Handler>();
	
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
	
	public void addLogHandler(Handler handler) {
	    mHandlers.add(handler);
	}
	
	private String getLogName() {
	    return mConnectionStr;
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

        mLogger = Logger.getLogger(getLogName());
        mLogger.setLevel(Level.FINEST);
        
        for (Handler handler : mHandlers) {
            mLogger.addHandler(handler);
        }
        
        for (GtfsRealTimeFeed feed : mFeeds) {
            feed.setLogger(mLogger);
        }
        
        Connection connection = null;
        GtfsRealTimeSqlRecorder recorder = null;

        mCurrentInterval = mDefaultInterval;
        
        while (true) {
			try {
			    if (connection == null) {
			        mLogger.info(String.format("Connecting to database: %s", mConnectionStr));
			        connection = DriverManager.getConnection(mConnectionStr, mUsername, mPassword);
			        recorder = new GtfsRealTimeSqlRecorder(mLogger, connection);
			        recorder.startup();
			    }
			    
				recorder.begin();

				for (GtfsRealTimeFeed feed : mFeeds) {
					try {
						feed.load();
					} catch (Exception e) {
					    mLogger.info(getString(e));
						continue;
					}

					try {
						recorder.record(feed.getFeedMessage());
					} catch (Exception e) {
                        mLogger.info(getString(e));
					}
				}

				boolean reconnect = recorder.getNumOpenQueries() > 0;

				if (!reconnect) {
				    recorder.commit();
				}

				if (reconnect) {
				    mLogger.warning(String.format("Disconnecting from %s", mConnectionStr));
				    connection.close();
				    connection = null;
				    
				    mCurrentInterval += mDefaultInterval;
				}
				else {
				    mCurrentInterval = mDefaultInterval;
				}
				
				if (mCurrentInterval > mMaxInterval) {
				    mLogger.warning(String.format("Interval too large (%dms), exiting", mCurrentInterval));
				    break;
				}
				else {
				    mLogger.info(String.format("Sleeping %dms", mCurrentInterval));
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
        
        for (Handler handler : mHandlers) {
            mLogger.removeHandler(handler);
            handler.close();
        }
        
        for (GtfsRealTimeFeed feed : mFeeds) {
            feed.setLogger(null);
        }
	}

    private String getString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        
        return sw.toString();
    }
}
