package com.transitfeeds.gtfsrealtimetosql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.transit.realtime.GtfsRealtime.Alert;
import com.google.transit.realtime.GtfsRealtime.EntitySelector;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.Position;
import com.google.transit.realtime.GtfsRealtime.TimeRange;
import com.google.transit.realtime.GtfsRealtime.TranslatedString;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import com.google.transit.realtime.GtfsRealtime.VehicleDescriptor;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition.CongestionLevel;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition.VehicleStopStatus;

public class GtfsRealTimeSqlRecorder {

	private Connection mConnection;
	private Map<String, PreparedStatement> mStatements = new HashMap<String, PreparedStatement>();

	public GtfsRealTimeSqlRecorder(Connection connection) {
		mConnection = connection;
	}

	public void startup() throws Exception {
		createTables();
		openStatements();
	}
	
	public void shutdown() throws Exception {
		closeStatements();
	}

	private boolean mAutoCommit;
	
	public void begin() throws SQLException {
		mAutoCommit = mConnection.getAutoCommit();
		mConnection.setAutoCommit(false);
		clearData();
	}
	
	public void commit() throws SQLException
	{
		mConnection.commit();
		mConnection.setAutoCommit(mAutoCommit);
	}
	
	public void record(FeedMessage feedMessage) throws Exception {

		for (FeedEntity entity : feedMessage.getEntityList()) {
			if (entity.hasAlert()) {
				try {
					recordAlert(entity.getAlert());
				} catch (SQLException e) {
				}
			}

			if (entity.hasTripUpdate()) {
				try {
					recordTripUpdate(entity.getTripUpdate());
				} catch (Exception e) {
				}
			}

			if (entity.hasVehicle()) {
				try {
					recordVehicle(entity.getVehicle());
				} catch (Exception e) {
				}
			}
		}
	}

	public static String[] TABLES = { 
		"gtfs_rt_alerts", "alert_id INTEGER PRIMARY KEY, header TEXT, description TEXT, cause INTEGER, effect INTEGER", "",
		"gtfs_rt_alerts_timeranges", "alert_id INTEGER, start INTEGER, finish INTEGER", "",
		"gtfs_rt_alerts_entities", "alert_id INTEGER, agency_id TEXT, route_id TEXT, route_type INTEGER, stop_id TEXT, trip_rship INTEGER, trip_start_date TEXT, trip_start_time TEXT, trip_id TEXT", "agency_id,route_id,stop_id,trip_id",
		"gtfs_rt_vehicles", "congestion INTEGER, status INTEGER, sequence INTEGER, bearing REAL, odometer REAL, speed REAL, latitude REAL, longitude REAL, stop_id TEXT, ts INTEGER, trip_sr INTEGER, trip_date TEXT, trip_time TEXT, trip_id TEXT, vehicle_id TEXT, vehicle_label TEXT, vehicle_plate TEXT", "stop_id,trip_id",
		"gtfs_rt_trip_updates", "update_id INTEGER PRIMARY KEY, ts INTEGER, trip_sr INTEGER, trip_date TEXT, trip_time TEXT, trip_id TEXT, vehicle_id TEXT, vehicle_label TEXT, vehicle_plate TEXT", "trip_id",
		"gtfs_rt_trip_updates_stoptimes", "update_id INTEGER, arrival_time INTEGER, arrival_uncertainty INTEGER, arrival_delay INTEGER, departure_time INTEGER, departure_uncertainty INTEGER, departure_delay INTEGER, rship INTEGER, stop_id TEXT, stop_sequence INTEGER", "stop_id"
	};
	
	private void clearData() throws SQLException {
		System.err.println("Clearing tables");
		
		for (int i = 0; i < TABLES.length; i += 3) {
			Statement stmt = mConnection.createStatement();
			stmt.execute("DELETE FROM " + TABLES[i]);
			stmt.close();
		}
	}

	private void createTables() throws SQLException {
		DatabaseMetaData meta = mConnection.getMetaData();
		
		ResultSet tables = meta.getTables(null, null, null, null);

		Set<String> tableNames = new HashSet<String>();
		
		while (tables.next()) {
			tableNames.add(tables.getString("TABLE_NAME"));
		}

		for (int i = 0; i < TABLES.length; i += 3) {
			String tableName = TABLES[i];
			
			if (tableNames.contains(tableName)) {
				continue;
			}

			System.err.println("Creating table " + tableName);

			String create = TABLES[i+1];
			create = create.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY");
			
			Statement stmt = mConnection.createStatement();
			stmt.execute(String.format("CREATE TABLE %s (%s)", tableName, create));
			stmt.close();
			
			String[] indexColumns = TABLES[i+2].split(",");
			
			if (indexColumns.length > 0) {
				stmt = mConnection.createStatement();
				
				for (int j = 0; j < indexColumns.length; j++) {
					String column = indexColumns[j];
							
					if (column.length() == 0) {
						continue;
					}
							
					stmt.execute(String.format("CREATE INDEX %s_%s ON %s (%s)", tableName, column, tableName, column));
				}
				
				stmt.close();
			}
		}
	}

	public static final String STALERT = "STALERT";
	public static final String STALERT_TIMERANGES = "STALERT_TIMERANGES";
	public static final String STALERT_ENTITIES = "STALERT_ENTITIES";
	public static final String STVEHICLE = "STVEHICLE";
	public static final String STTRIPUPDATE = "STTRIPUPDATE";
	public static final String STTRIPUPDATE_STOPTIMEUPDATES = "STTRIPUPDATE_STOPTIMEUPDATES";

	private void openStatements() throws SQLException {
		mStatements.put(STALERT, mConnection.prepareStatement("INSERT INTO gtfs_rt_alerts (header, description, cause, effect) VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
		mStatements.put(STALERT_TIMERANGES, mConnection.prepareStatement("INSERT INTO gtfs_rt_alerts_timeranges (alert_id, start, finish) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
		mStatements.put(STALERT_ENTITIES, mConnection.prepareStatement("INSERT INTO gtfs_rt_alerts_entities (alert_id, agency_id, route_id, route_type, stop_id, trip_rship, trip_start_date, trip_start_time, trip_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
		mStatements.put(STVEHICLE, mConnection.prepareStatement("INSERT INTO gtfs_rt_vehicles (congestion, status, sequence, bearing, odometer, speed, latitude, longitude, stop_id, ts, trip_sr, trip_date, trip_time, trip_id, vehicle_id, vehicle_label, vehicle_plate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
		mStatements.put(STTRIPUPDATE, mConnection.prepareStatement("INSERT INTO gtfs_rt_trip_updates (ts, trip_sr, trip_date, trip_time, trip_id, vehicle_id, vehicle_label, vehicle_plate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
		mStatements.put(STTRIPUPDATE_STOPTIMEUPDATES, mConnection.prepareStatement("INSERT INTO gtfs_rt_trip_updates_stoptimes (update_id, arrival_time, arrival_uncertainty, arrival_delay, departure_time, departure_uncertainty, departure_delay, rship, stop_id, stop_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
	}

	private void closeStatements() throws SQLException {
		for (PreparedStatement stmt : mStatements.values()) {
			stmt.close();
		}

		mStatements = new HashMap<String, PreparedStatement>();
	}

	private void recordVehicle(VehiclePosition vehicle) throws SQLException, Exception {
		if (!vehicle.hasPosition()) {
			throw new Exception("No position found");
		}
		
		System.err.println("Recording vehicle position");
		
		PreparedStatement stmt = mStatements.get(STVEHICLE);

		stmt.clearParameters();

		stmt.setInt(1, vehicle.hasCongestionLevel() ? vehicle.getCongestionLevel().getNumber() : CongestionLevel.UNKNOWN_CONGESTION_LEVEL_VALUE);
		stmt.setInt(2, vehicle.hasCurrentStatus() ? vehicle.getCurrentStatus().getNumber() : VehicleStopStatus.IN_TRANSIT_TO_VALUE);
		stmt.setInt(3, vehicle.hasCurrentStopSequence() ? vehicle.getCurrentStopSequence() : -1);
		
		Position pos = vehicle.getPosition();
		
		if (pos.hasBearing()) {
			stmt.setFloat(4, pos.getBearing());
		}
		else {
			stmt.setNull(4, Types.FLOAT);
		}

		if (pos.hasOdometer()) {
			stmt.setDouble(5, pos.getOdometer());
		}
		else {
			stmt.setNull(5, Types.DOUBLE);
		}

		if (pos.hasSpeed()) {
			stmt.setFloat(6, pos.getSpeed());
		}
		else {
			stmt.setNull(6, Types.FLOAT);
		}

		if (pos.hasLatitude()) {
			stmt.setFloat(7, pos.getLatitude());
		}
		else {
			stmt.setNull(7, Types.FLOAT);
		}

		if (pos.hasLongitude()) {
			stmt.setFloat(8, pos.getLongitude());
		}
		else {
			stmt.setNull(8, Types.FLOAT);
		}
		
		if (vehicle.hasStopId()) {
			stmt.setString(9, vehicle.getStopId());
		}
		else {
			stmt.setNull(9, Types.VARCHAR);
		}

		if (vehicle.hasTimestamp()) {
			stmt.setLong(10, vehicle.getTimestamp());
		}
		else {
			stmt.setNull(10, Types.INTEGER);
		}
		
		if (vehicle.hasTrip()) {
			TripDescriptor trip = vehicle.getTrip();
			
			if (trip.hasScheduleRelationship()) {
				stmt.setInt(11, trip.getScheduleRelationship().getNumber());
			}
			else {
				stmt.setNull(11, Types.INTEGER);
			}

			if (trip.hasStartDate()) {
				stmt.setString(12, trip.getStartDate());
			}
			else {
				stmt.setNull(12, Types.VARCHAR);
			}
			
			if (trip.hasStartTime()) {
				stmt.setString(13, trip.getStartTime());
			}
			else {
				stmt.setNull(13, Types.VARCHAR);
			}

			if (trip.hasTripId()) {
				stmt.setString(14, trip.getTripId());
			}
			else {
				stmt.setNull(14, Types.VARCHAR);
			}
		}
		else {
			stmt.setNull(11, Types.INTEGER);
			stmt.setNull(12, Types.VARCHAR);
			stmt.setNull(13, Types.VARCHAR);
			stmt.setNull(14, Types.VARCHAR);
		}
		
		if (vehicle.hasVehicle()) {
			VehicleDescriptor vd = vehicle.getVehicle();
			
			if (vd.hasId()) {
				stmt.setString(15, vd.getId());
			}
			else {
				stmt.setNull(15, Types.VARCHAR);
			}

			if (vd.hasLabel()) {
				stmt.setString(16, vd.getLabel());
			}
			else {
				stmt.setNull(16, Types.VARCHAR);
			}

			if (vd.hasLicensePlate()) {
				stmt.setString(17, vd.getLicensePlate());
			}
			else {
				stmt.setNull(17, Types.VARCHAR);
			}
		}
		else {
			stmt.setNull(15, Types.VARCHAR);
			stmt.setNull(16, Types.VARCHAR);
			stmt.setNull(17, Types.VARCHAR);
		}

		stmt.execute();
	}

	private void recordTripUpdate(TripUpdate tripUpdate) throws SQLException {
		System.err.println("Recording trip update");

		PreparedStatement stmt = mStatements.get(STTRIPUPDATE);

		stmt.clearParameters();

		
		if (tripUpdate.hasTimestamp()) {
			stmt.setLong(1, tripUpdate.getTimestamp());
		}
		else {
			stmt.setNull(1, Types.INTEGER);
		}

		if (tripUpdate.hasTrip()) {
			TripDescriptor trip = tripUpdate.getTrip();
			
			if (trip.hasScheduleRelationship()) {
				stmt.setInt(2, trip.getScheduleRelationship().getNumber());
			}
			else {
				stmt.setNull(2, Types.INTEGER);
			}
			
			if (trip.hasStartDate()) {
				stmt.setString(3, trip.getStartDate());
			}
			else {
				stmt.setNull(3, Types.VARCHAR);
			}

			if (trip.hasStartTime()) {
				stmt.setString(4, trip.getStartTime());
			}
			else {
				stmt.setNull(4, Types.VARCHAR);
			}

			if (trip.hasTripId()) {
				stmt.setString(5, trip.getTripId());
			}
			else {
				stmt.setNull(5, Types.VARCHAR);
			}
		}
		else {
			stmt.setNull(2, Types.INTEGER);
			stmt.setNull(3, Types.VARCHAR);
			stmt.setNull(4, Types.VARCHAR);
			stmt.setNull(5, Types.VARCHAR);
		}
		
		if (tripUpdate.hasVehicle()) {
			VehicleDescriptor vd = tripUpdate.getVehicle();
			
			if (vd.hasId()) {
				stmt.setString(6, vd.getId());
			}
			else {
				stmt.setNull(6, Types.INTEGER);
			}
			
			if (vd.hasLabel()) {
				stmt.setString(7, vd.getLabel());
			}
			else {
				stmt.setNull(7, Types.VARCHAR);
			}

			if (vd.hasLicensePlate()) {
				stmt.setString(8, vd.getLicensePlate());
			}
			else {
				stmt.setNull(8, Types.VARCHAR);
			}
		}
		else {
			stmt.setNull(6, Types.INTEGER);
			stmt.setNull(7, Types.VARCHAR);
			stmt.setNull(8, Types.VARCHAR);
		}

		stmt.execute();
		
		ResultSet rs = stmt.getGeneratedKeys();
		rs.next();
		
		int updateId = rs.getInt(1);

		stmt = mStatements.get(STTRIPUPDATE_STOPTIMEUPDATES);
		
		for (StopTimeUpdate stu : tripUpdate.getStopTimeUpdateList()) {
			stmt.clearParameters();
			stmt.setInt(1, updateId);

			if (stu.hasArrival()) {
				StopTimeEvent ste = stu.getArrival();
				
				if (ste.hasTime()) {
					stmt.setLong(2, ste.getTime());
				}
				else {
					stmt.setNull(2, Types.INTEGER);
				}

				if (ste.hasUncertainty()) {
					stmt.setInt(3, ste.getUncertainty());
				}
				else {
					stmt.setNull(3, Types.INTEGER);
				}
				
				if (ste.hasDelay()) {
					stmt.setInt(4, ste.getDelay());
				}
				else {
					stmt.setNull(4, Types.INTEGER);
				}
			}
			else {
				stmt.setNull(2, Types.INTEGER);
				stmt.setNull(3, Types.INTEGER);
				stmt.setNull(4, Types.INTEGER);
			}
			
			if (stu.hasDeparture()) {
				StopTimeEvent ste = stu.getDeparture();
				
				if (ste.hasTime()) {
					stmt.setLong(5, ste.getTime());
				}
				else {
					stmt.setNull(5, Types.INTEGER);
				}

				if (ste.hasUncertainty()) {
					stmt.setInt(6, ste.getUncertainty());
				}
				else {
					stmt.setNull(6, Types.INTEGER);
				}

				if (ste.hasDelay()) {
					stmt.setInt(7, ste.getDelay());
				}
				else {
					stmt.setNull(7, Types.INTEGER);
				}
			}
			else {
				stmt.setNull(5, Types.INTEGER);
				stmt.setNull(6, Types.INTEGER);
				stmt.setNull(7, Types.INTEGER);
			}
			
			stmt.setInt(8, stu.hasScheduleRelationship() ? stu.getScheduleRelationship().getNumber() : com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED_VALUE);
			
			if (stu.hasStopId()) {
				stmt.setString(9, stu.getStopId());
			}
			else {
				stmt.setNull(9, Types.VARCHAR);
			}

			stmt.setInt(10, stu.hasStopSequence() ? stu.getStopSequence() : -1);

			stmt.execute();
		}
	}

	private void recordAlert(Alert alert) throws SQLException {
		System.err.println("Recording alert");

		PreparedStatement stmt = mStatements.get(STALERT);

		stmt.clearParameters();

		if (alert.hasHeaderText()) {
			stmt.setString(1, getString(alert.getHeaderText()));
		}
		else {
			stmt.setNull(1, Types.VARCHAR);
		}

		if (alert.hasDescriptionText()) {
			stmt.setString(2, getString(alert.getDescriptionText()));
		}
		else {
			stmt.setNull(2, Types.VARCHAR);
		}

		if (alert.hasCause()) {
			stmt.setInt(3, alert.getCause().getNumber());
		}
		else {
			stmt.setNull(3, Types.INTEGER);
		}

		if (alert.hasEffect()) {
			stmt.setInt(4, alert.getEffect().getNumber());
		}
		else {
			stmt.setNull(4, Types.INTEGER);
		}

		stmt.execute();

		ResultSet rs = stmt.getGeneratedKeys();
		rs.next();

		int alertId = rs.getInt(1);

		stmt = mStatements.get(STALERT_TIMERANGES);

		for (TimeRange timeRange : alert.getActivePeriodList()) {
			stmt.clearParameters();
			stmt.setInt(1, alertId);

			if (timeRange.hasStart()) {
				stmt.setLong(2, timeRange.getStart());
			}
			else {
				stmt.setNull(2, Types.INTEGER);
			}

			if (timeRange.hasEnd()) {
				stmt.setLong(3, timeRange.getEnd());
			}
			else {
				stmt.setNull(3, Types.INTEGER);
			}

			stmt.execute();
		}

		stmt = mStatements.get(STALERT_ENTITIES);

		for (EntitySelector entity : alert.getInformedEntityList()) {
			stmt.clearParameters();

			stmt.setInt(1, alertId);

			if (entity.hasAgencyId()) {
				stmt.setString(2, entity.getAgencyId());
			}
			else {
				stmt.setNull(2, Types.VARCHAR);
			}

			if (entity.hasRouteId()) {
				stmt.setString(3, entity.getRouteId());
			}
			else {
				stmt.setNull(3, Types.VARCHAR);
			}

			if (entity.hasRouteType()) {
				stmt.setInt(4, entity.getRouteType());
			}
			else {
				stmt.setNull(4, Types.INTEGER);
			}

			if (entity.hasStopId()) {
				stmt.setString(5, entity.getStopId());
			}
			else {
				stmt.setNull(5, Types.VARCHAR);
			}

			if (entity.hasTrip()) {
				TripDescriptor trip = entity.getTrip();

				if (trip.hasScheduleRelationship()) {
					stmt.setInt(6, trip.getScheduleRelationship().getNumber());
				}
				else {
					stmt.setNull(6, Types.INTEGER);
				}

				if (trip.hasStartDate()) {
					stmt.setString(7, trip.getStartDate());
				}
				else {
					stmt.setNull(7, Types.VARCHAR);
				}

				if (trip.hasStartTime()) {
					stmt.setString(8, trip.getStartTime());
				}
				else {
					stmt.setNull(8, Types.VARCHAR);
				}

				if (trip.hasTripId()) {
					stmt.setString(9, trip.getTripId());
				}
				else {
					stmt.setNull(9, Types.VARCHAR);
				}
			}
			else {
				stmt.setNull(6, Types.INTEGER);
				stmt.setNull(7, Types.VARCHAR);
				stmt.setNull(8, Types.VARCHAR);
				stmt.setNull(9, Types.VARCHAR);
			}

			stmt.execute();
		}
	}

	private String getString(TranslatedString str) {
		return str.getTranslation(0).getText();
	}
}
