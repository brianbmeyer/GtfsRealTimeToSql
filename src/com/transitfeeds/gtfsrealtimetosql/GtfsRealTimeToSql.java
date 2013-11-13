package com.transitfeeds.gtfsrealtimetosql;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class GtfsRealTimeToSql {

	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption("u", true, "GTFS-RealTime URL");
		options.addOption("s", true, "JDBC Connection");
		options.addOption("d", false, "Daemonize");
		options.addOption("dbusername", true, "Database username");
		options.addOption("dbpassword", true, "Database password");
		options.addOption("username", true, "Username");
		options.addOption("password", true, "Password");
		options.addOption("refresh", true, "Refresh seconds");

		CommandLineParser parser = new GnuParser();
		CommandLine line = parser.parse(options, args);

		if (!line.hasOption("u")) {
			System.err.println("GTFS-RealTime URL must be specified");
			showHelp(options);
			System.exit(1);
		}
		
		if (!line.hasOption("s")) {
			System.err.println("JDBC path must be specified, examples:");
			System.err.println("\tPostgreSQL: jdbc:postgresql://localhost/dbname");
			System.err.println("\tSqlite:     jdbc:sqlite:/path/to/db.sqlite");
			showHelp(options);
			System.exit(2);
		}

		if (line.hasOption("username") && !line.hasOption("password")) {
			System.out.println("Password must be specified");
			System.exit(1);
			return;
		}
		
		if (line.hasOption("d")) {
			daemonize();
		}
		
		int seconds = 0;
		
		try {
			seconds = Integer.valueOf(line.getOptionValue("refresh"));
		}
		catch (Exception e) {
			
		}
		
		seconds = Math.max(15, seconds);

		String username = line.getOptionValue("username");
		String password = line.getOptionValue("password");

		String url = line.getOptionValue("u");

		String connStr = line.getOptionValue("s");
		
		if (connStr.startsWith("jdbc:sqlite:")) {
			// may not work without this call
			Class.forName("org.sqlite.JDBC");
		}
		
		Connection connection = DriverManager.getConnection(connStr, line.getOptionValue("dbusername"), line.getOptionValue("dbpassword"));
		
		GtfsRealTimeFeed feed = new GtfsRealTimeFeed(url);
		feed.setCredentials(username, password);

		GtfsRealTimeSqlRecorder recorder = new GtfsRealTimeSqlRecorder(connection);
		recorder.startup();

		while (true) {
			try {
				feed.load();
				recorder.record(feed.getFeedMessage());
				
				long sleep = seconds * 1000;
				
				System.err.println(String.format("Sleeping %dms", sleep));
				
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				break;
			}
		}
		
		recorder.shutdown();
	}

	public static void showHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("GtfsRealTimeToSql", options);
	}

//	private static Thread mMainThread;

	public static void daemonize() {
//		mMainThread = Thread.currentThread();

		File pid = getPidFile();

		if (pid != null) {
			getPidFile().deleteOnExit();
		}

		System.out.close();
		System.err.close();
	}

	static public File getPidFile() {
		String pidPath = System.getProperty("daemon.pidfile");

		if (pidPath == null) {
			return null;
		}

		return new File(pidPath);
	}
}
