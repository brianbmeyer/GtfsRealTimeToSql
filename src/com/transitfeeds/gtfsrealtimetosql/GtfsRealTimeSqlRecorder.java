package com.transitfeeds.gtfsrealtimetosql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

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

    private Connection                     mConnection;
    private Map<String, PreparedStatement> mStatements    = new HashMap<String, PreparedStatement>();

    private static final String            COPY_SEPARATOR = ",";
    
    private int mOpenQueries = 0;

    public GtfsRealTimeSqlRecorder(Connection connection) {
        mConnection = connection;
    }

    public void startup() throws SQLException {
        createTables();
        openStatements();
    }

    public void shutdown() throws SQLException {
        closeStatements();
    }

    private boolean mAutoCommit;

    public void begin() throws SQLException {
        mAutoCommit = mConnection.getAutoCommit();
        mConnection.setAutoCommit(false);

        resetUpdateId();
    }

    public void commit() throws SQLException {
        mConnection.commit();
        mConnection.setAutoCommit(mAutoCommit);
    }
    
    public int getNumOpenQueries() {
        return mOpenQueries;
    }

    public void record(FeedMessage feedMessage) throws SQLException {
        boolean hasAlerts = false;
        boolean hasTripUpdates = false;
        boolean hasVehiclePositions = false;
        
        mOpenQueries = 0;

        for (FeedEntity entity : feedMessage.getEntityList()) {
            if (entity.hasAlert()) {
                hasAlerts = true;
            }

            if (entity.hasTripUpdate()) {
                hasTripUpdates = true;
            }

            if (entity.hasVehicle()) {
                hasVehiclePositions = true;
            }
        }

        System.err.println("Clearing tables...");

        if (hasAlerts) {
            clearAlertsData();
        }

        if (hasTripUpdates) {
            clearTripUpdatesData();
        }

        if (hasVehiclePositions) {
            clearVehiclePositionsData();
        }

        hasAlerts = false;
        hasTripUpdates = false;
        
        System.err.println("Finished clearing tables");

        boolean useCopy = mConnection instanceof BaseConnection;

        CopyManager cm = null;
        
        DataCopier tuCopier = null;
        DataCopier stCopier = null;
        DataCopier vpCopier = null;
        
        CopyIn tuCopyIn = null;
        CopyIn stCopyIn = null;
        CopyIn vpCopyIn = null;

        if (useCopy) {
            cm = new CopyManager((BaseConnection) mConnection);
            tuCopier = new DataCopier();
            stCopier = new DataCopier();
            vpCopier = new DataCopier();
            
            if (hasTripUpdates) {
                stCopyIn = cm.copyIn(COPY_TRIP_UPDATES_STOP_TIMES);
                mOpenQueries++;
                
                stCopier = new DataCopier(stCopyIn, COPY_SEPARATOR);
            }
            else {
                vpCopyIn = cm.copyIn(COPY_VEHICLE_POSITIONS);
                mOpenQueries++;
                
                vpCopier = new DataCopier(vpCopyIn, COPY_SEPARATOR);
            }
        }

        for (FeedEntity entity : feedMessage.getEntityList()) {
            if (hasAlerts) {
                try {
                    recordAlert(entity.getAlert());
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (hasTripUpdates) {
                try {
                    recordTripUpdate(entity.getTripUpdate(), tuCopier, stCopier);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (hasVehiclePositions) {
                try {
                    recordVehicle(entity.getVehicle(), vpCopier);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if (hasAlerts) {
            System.err.print("Committing alerts... ");
            
            try {
                mStatements.get(STALERT).executeBatch();
                mStatements.get(STALERT_ENTITIES).executeBatch();
                mStatements.get(STALERT_TIMERANGES).executeBatch();
            }
            catch (Exception e) {
                
            }
            System.err.println("done");
        }

        if (hasTripUpdates) {
            System.err.print("Committing trip updates... ");

            try {
                if (stCopier == null) {
                    mStatements.get(STTRIPUPDATE_STOPTIMEUPDATES).executeBatch();
                }
                else if (stCopyIn == null && stCopier.size() > 0) {
                    stCopyIn = cm.copyIn(COPY_TRIP_UPDATES_STOP_TIMES);
                    mOpenQueries++;
                    
                    stCopier.write(stCopyIn, COPY_SEPARATOR);
                }
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
            
            if (stCopyIn != null) {
                try {
                    stCopyIn.endCopy();
                    mOpenQueries--;
                }
                catch (Exception e) {
                    
                }
            }

            try {
                if (tuCopier == null) {
                    mStatements.get(STTRIPUPDATE).executeBatch();
                }
                else if (tuCopyIn == null && tuCopier.size() > 0) {
                    tuCopyIn = cm.copyIn(COPY_TRIP_UPDATES);
                    mOpenQueries++;
                    
                    tuCopier.write(tuCopyIn, COPY_SEPARATOR);
                }
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
            
            if (tuCopyIn != null) {
                try {
                    tuCopyIn.endCopy();
                    mOpenQueries--;
                }
                catch (Exception e) {
                    
                }
            }

            System.err.println("done");
        }

        if (hasVehiclePositions) {
            System.err.print("Committing vehicle positions... ");

            try {
                if (vpCopier == null) {
                    mStatements.get(STVEHICLE).executeBatch();
                }
                else if (vpCopyIn == null && vpCopier.size() > 0) {
                    vpCopyIn = cm.copyIn(COPY_VEHICLE_POSITIONS);
                    mOpenQueries++;
                    vpCopier.write(vpCopyIn, COPY_SEPARATOR);
                }
            }
            catch (Exception e) {
                e.printStackTrace();                
            }
            
            if (vpCopyIn != null) {
                vpCopyIn.endCopy();
                mOpenQueries--;
            }

            System.err.println("done");
        }
    }

    public static String[] TABLES = {
            "gtfs_rt_alerts", "alert_id INTEGER, header TEXT, description TEXT, cause INTEGER, effect INTEGER, recorded INTEGER", "",
            "gtfs_rt_alerts_timeranges", "alert_id INTEGER, start INTEGER, finish INTEGER", "",
            "gtfs_rt_alerts_entities", "alert_id INTEGER, agency_id TEXT, route_id TEXT, route_type INTEGER, stop_id TEXT, trip_rship INTEGER, trip_start_date TEXT, trip_start_time TEXT, trip_id TEXT", "agency_id,route_id,stop_id,trip_id",
            "gtfs_rt_vehicles", "congestion INTEGER, status INTEGER, sequence INTEGER, bearing REAL, odometer REAL, speed REAL, latitude REAL, longitude REAL, stop_id TEXT, ts INTEGER, trip_sr INTEGER, trip_date TEXT, trip_time TEXT, trip_id TEXT, route_id TEXT, vehicle_id TEXT, vehicle_label TEXT, vehicle_plate TEXT, recorded INTEGER", "stop_id,trip_id,route_id",
            "gtfs_rt_trip_updates", "update_id INTEGER, ts INTEGER, trip_sr INTEGER, trip_date TEXT, trip_time TEXT, trip_id TEXT, route_id TEXT, vehicle_id TEXT, vehicle_label TEXT, vehicle_plate TEXT, recorded INTEGER", "update_id,trip_id,route_id",
            "gtfs_rt_trip_updates_stoptimes", "update_id INTEGER, arrival_time INTEGER, arrival_uncertainty INTEGER, arrival_delay INTEGER, departure_time INTEGER, departure_uncertainty INTEGER, departure_delay INTEGER, rship INTEGER, stop_id TEXT, stop_sequence INTEGER", "stop_id,update_id"  
    };

    private void clearTripUpdatesData() throws SQLException {
        clearData(4, 5);
    }

    private void clearAlertsData() throws SQLException {
        clearData(0, 2);
    }

    private void clearVehiclePositionsData() throws SQLException {
        clearData(3, 3);
    }

    private void clearData(int from, int to) throws SQLException {
        for (int i = from * 3; i <= to * 3; i += 3) {
            String query = "DELETE FROM " + TABLES[i];
            System.err.println(query);

            Statement stmt = mConnection.createStatement();
            stmt.execute(query);
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

            String create = TABLES[i + 1];
            create = create.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY");

            Statement stmt = mConnection.createStatement();
            stmt.execute(String.format("CREATE TABLE %s (%s)", tableName, create));
            stmt.close();

            String[] indexColumns = TABLES[i + 2].split(",");

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

    public static final String STALERT                      = "STALERT";
    public static final String STALERT_TIMERANGES           = "STALERT_TIMERANGES";
    public static final String STALERT_ENTITIES             = "STALERT_ENTITIES";
    public static final String STVEHICLE                    = "STVEHICLE";
    public static final String STTRIPUPDATE                 = "STTRIPUPDATE";
    public static final String STTRIPUPDATE_STOPTIMEUPDATES = "STTRIPUPDATE_STOPTIMEUPDATES";

    private void openStatements() throws SQLException {
        mStatements.put(STALERT, mConnection.prepareStatement("INSERT INTO gtfs_rt_alerts (alert_id, header, description, cause, effect, recorded) VALUES (?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
        mStatements.put(STALERT_TIMERANGES, mConnection.prepareStatement("INSERT INTO gtfs_rt_alerts_timeranges (alert_id, start, finish) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
        mStatements.put(STALERT_ENTITIES, mConnection.prepareStatement("INSERT INTO gtfs_rt_alerts_entities (alert_id, agency_id, route_id, route_type, stop_id, trip_rship, trip_start_date, trip_start_time, trip_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
        mStatements.put(STVEHICLE, mConnection.prepareStatement("INSERT INTO gtfs_rt_vehicles (congestion, status, sequence, bearing, odometer, speed, latitude, longitude, stop_id, ts, trip_sr, trip_date, trip_time, trip_id, route_id, vehicle_id, vehicle_label, vehicle_plate, recorded) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
        mStatements.put(STTRIPUPDATE, mConnection.prepareStatement("INSERT INTO gtfs_rt_trip_updates (update_id, ts, trip_sr, trip_date, trip_time, trip_id, route_id, vehicle_id, vehicle_label, vehicle_plate, recorded) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
        mStatements.put(STTRIPUPDATE_STOPTIMEUPDATES, mConnection.prepareStatement("INSERT INTO gtfs_rt_trip_updates_stoptimes (update_id, arrival_time, arrival_uncertainty, arrival_delay, departure_time, departure_uncertainty, departure_delay, rship, stop_id, stop_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS));
    }

    private static final String COPY_TRIP_UPDATES            = "COPY gtfs_rt_trip_updates(update_id, ts, trip_sr, trip_date, trip_time, trip_id, route_id, vehicle_id, vehicle_label, vehicle_plate, recorded) FROM STDIN WITH DELIMITER '" + COPY_SEPARATOR + "' NULL AS ''";
    private static final String COPY_TRIP_UPDATES_STOP_TIMES = "COPY gtfs_rt_trip_updates_stoptimes(update_id, arrival_time, arrival_uncertainty, arrival_delay, departure_time, departure_uncertainty, departure_delay, rship, stop_id, stop_sequence) FROM STDIN WITH DELIMITER '" + COPY_SEPARATOR + "' NULL AS ''";
    private static final String COPY_VEHICLE_POSITIONS       = "COPY gtfs_rt_vehicles(congestion, status, sequence, bearing, odometer, speed, latitude, longitude, stop_id, ts, trip_sr, trip_date, trip_time, trip_id, route_id, vehicle_id, vehicle_label, vehicle_plate, recorded) FROM STDIN WITH DELIMITER '" + COPY_SEPARATOR + "' NULL AS ''";

    private void closeStatements() throws SQLException {
        for (PreparedStatement stmt : mStatements.values()) {
            stmt.close();
        }

        mStatements = new HashMap<String, PreparedStatement>();
    }

    private void recordVehicle(VehiclePosition vehicle, DataCopier copier) throws SQLException, Exception {
        if (!vehicle.hasPosition()) {
            throw new Exception("No position found");
        }

        PreparedStatement stmt = null;

        DataCopierRow row = null;

        if (copier == null) {
            stmt = mStatements.get(STVEHICLE);
        }
        else {
            row = new DataCopierRow();
        }

        int congestionLevel = vehicle.hasCongestionLevel() ? vehicle.getCongestionLevel().getNumber() : CongestionLevel.UNKNOWN_CONGESTION_LEVEL_VALUE;
        int vehicleStatus = vehicle.hasCurrentStatus() ? vehicle.getCurrentStatus().getNumber() : VehicleStopStatus.IN_TRANSIT_TO_VALUE;
        int stopSequence = vehicle.hasCurrentStopSequence() ? vehicle.getCurrentStopSequence() : -1;

        if (row == null) {
            stmt.setInt(1, congestionLevel);
            stmt.setInt(2, vehicleStatus);
            stmt.setInt(3, stopSequence);
        }
        else {
            row.add(congestionLevel);
            row.add(vehicleStatus);
            row.add(stopSequence);
        }

        Position pos = vehicle.getPosition();

        if (pos.hasBearing()) {
            if (row == null) {
                stmt.setFloat(4, pos.getBearing());
            }
            else {
                row.add(pos.getBearing());
            }
        }
        else {
            if (row == null) {
                stmt.setNull(4, Types.FLOAT);
            }
            else {
                row.addNull();
            }
        }

        if (pos.hasOdometer()) {
            if (row == null) {
                stmt.setDouble(5, pos.getOdometer());
            }
            else {
                row.add(pos.getOdometer());
            }
        }
        else {
            if (row == null) {
                stmt.setNull(5, Types.DOUBLE);
            }
            else {
                row.addNull();
            }
        }

        if (pos.hasSpeed()) {
            if (row == null) {
                stmt.setFloat(6, pos.getSpeed());
            }
            else {
                row.add(pos.getSpeed());
            }
        }
        else {
            if (row == null) {
                stmt.setNull(6, Types.FLOAT);
            }
            else {
                row.addNull();
            }
        }

        if (pos.hasLatitude()) {
            if (row == null) {
                stmt.setFloat(7, pos.getLatitude());
            }
            else {
                row.add(pos.getLatitude());
            }
        }
        else {
            if (row == null) {
                stmt.setNull(7, Types.FLOAT);
            }
            else {
                row.addNull();
            }
        }

        if (pos.hasLongitude()) {
            if (row == null) {
                stmt.setFloat(8, pos.getLongitude());
            }
            else {
                row.add(pos.getLongitude());
            }
        }
        else {
            if (row == null) {
                stmt.setNull(8, Types.FLOAT);
            }
            else {
                row.addNull();
            }
        }

        if (vehicle.hasStopId()) {
            if (row == null) {
                stmt.setString(9, vehicle.getStopId());
            }
            else {
                row.add(vehicle.getStopId());
            }
        }
        else {
            if (row == null) {
                stmt.setNull(9, Types.VARCHAR);
            }
            else {
                row.addNull();
            }
        }

        if (vehicle.hasTimestamp()) {
            if (row == null) {
                stmt.setLong(10, vehicle.getTimestamp());
            }
            else {
                row.add(vehicle.getTimestamp());
            }
        }
        else {
            if (row == null) {
                stmt.setNull(10, Types.INTEGER);
            }
            else {
                row.addNull();
            }
        }

        if (vehicle.hasTrip()) {
            TripDescriptor trip = vehicle.getTrip();

            if (trip.hasScheduleRelationship()) {
                if (row == null) {
                    stmt.setInt(11, trip.getScheduleRelationship().getNumber());
                }
                else {
                    row.add(trip.getScheduleRelationship().getNumber());
                }
            }
            else {
                if (row == null) {
                    stmt.setNull(11, Types.INTEGER);
                }
                else {
                    row.addNull();
                }
            }

            if (trip.hasStartDate()) {
                if (row == null) {
                    stmt.setString(12, trip.getStartDate());
                }
                else {
                    row.add(trip.getStartDate());
                }
            }
            else {
                if (row == null) {
                    stmt.setNull(12, Types.VARCHAR);
                }
                else {
                    row.addNull();
                }
            }

            if (trip.hasStartTime()) {
                if (row == null) {
                    stmt.setString(13, trip.getStartTime());
                }
                else {
                    row.add(trip.getStartTime());
                }
            }
            else {
                if (row == null) {
                    stmt.setNull(13, Types.VARCHAR);
                }
                else {
                    row.addNull();
                }
            }

            if (trip.hasTripId()) {
                if (row == null) {
                    stmt.setString(14, trip.getTripId());
                }
                else {
                    row.add(trip.getTripId());
                }
            }
            else {
                if (row == null) {
                    stmt.setNull(14, Types.VARCHAR);
                }
                else {
                    row.addNull();
                }
            }
            
            if (trip.hasRouteId()) {
                if (row == null) {
                    stmt.setString(15, trip.getRouteId());
                }
                else {
                    row.add(trip.getRouteId());
                }
            }
            else {
                if (row == null) {
                    stmt.setNull(15, Types.VARCHAR);
                }
                else {
                    row.addNull();
                }
            }
        }
        else {
            if (row == null) {
                stmt.setNull(11, Types.INTEGER);
                stmt.setNull(12, Types.VARCHAR);
                stmt.setNull(13, Types.VARCHAR);
                stmt.setNull(14, Types.VARCHAR);
            }
            else {
                row.addNull(4);
            }
        }
        
        if (vehicle.hasVehicle()) {
            VehicleDescriptor vd = vehicle.getVehicle();

            if (vd.hasId()) {
                if (row == null) {
                    stmt.setString(16, vd.getId());
                }
                else {
                    row.add(vd.getId());
                }
            }
            else {
                if (row == null) {
                    stmt.setNull(16, Types.VARCHAR);
                }
                else {
                    row.addNull();
                }
            }

            if (vd.hasLabel()) {
                if (row == null) {
                    stmt.setString(17, vd.getLabel());
                }
                else {
                    row.add(vd.getLabel());
                }
            }
            else {
                if (row == null) {
                    stmt.setNull(17, Types.VARCHAR);
                }
                else {
                    row.addNull();
                }
            }

            if (vd.hasLicensePlate()) {
                if (row == null) {
                    stmt.setString(18, vd.getLicensePlate());
                }
                else {
                    row.add(vd.getLicensePlate());
                }
            }
            else {
                if (row == null) {
                    stmt.setNull(18, Types.VARCHAR);
                }
                else {
                    row.addNull();
                }
            }
        }
        else {
            if (row == null) {
                stmt.setNull(16, Types.VARCHAR);
                stmt.setNull(17, Types.VARCHAR);
                stmt.setNull(18, Types.VARCHAR);
            }
            else {
                row.addNull(3);
            }
        }

        Date recorded = new Date();
        
        if (row == null) {
            stmt.setInt(19, (int) (recorded.getTime() / 1000));
        }
        else {
            row.add((int) (recorded.getTime() / 1000));
        }

        if (stmt == null) {
            copier.add(row);
        }
        else {
            stmt.execute();
        }
    }

    private int mUpdateId = 0;

    private void resetUpdateId() {
        mUpdateId = 0;
    }

    private int getUpdateId() {
        return ++mUpdateId;
    }

    private void recordTripUpdate(TripUpdate tripUpdate, DataCopier tuCopier, DataCopier stCopier) throws SQLException {
        PreparedStatement stmt = null;

        int updateId = getUpdateId();

        DataCopierRow tuRow = null;

        if (tuCopier != null) {
            tuRow = new DataCopierRow();
        }
        else {
            stmt = mStatements.get(STTRIPUPDATE);
        }

        if (tuRow == null) {
            stmt.setInt(1, updateId);
        }
        else {
            tuRow.add(updateId);
        }

        if (tripUpdate.hasTimestamp()) {
            if (tuRow == null) {
                stmt.setLong(2, tripUpdate.getTimestamp());
            }
            else {
                tuRow.add(tripUpdate.getTimestamp());
            }
        }
        else {
            if (tuRow == null) {
                stmt.setNull(2, Types.INTEGER);
            }
            else {
                tuRow.addNull();
            }
        }

        if (tripUpdate.hasTrip()) {
            TripDescriptor trip = tripUpdate.getTrip();

            if (trip.hasScheduleRelationship()) {
                if (tuRow == null) {
                    stmt.setInt(3, trip.getScheduleRelationship().getNumber());
                }
                else {
                    tuRow.add(trip.getScheduleRelationship().getNumber());
                }
            }
            else {
                if (tuRow == null) {
                    stmt.setNull(3, Types.INTEGER);
                }
                else {
                    tuRow.addNull();
                }
            }

            if (trip.hasStartDate()) {
                if (tuRow == null) {
                    stmt.setString(4, trip.getStartDate());
                }
                else {
                    tuRow.add(trip.getStartDate());
                }
            }
            else {
                if (tuRow == null) {
                    stmt.setNull(4, Types.VARCHAR);
                }
                else {
                    tuRow.addNull();
                }
            }

            if (trip.hasStartTime()) {
                if (tuRow == null) {
                    stmt.setString(5, trip.getStartTime());
                }
                else {
                    tuRow.add(trip.getStartTime());
                }
            }
            else {
                if (tuRow == null) {
                    stmt.setNull(5, Types.VARCHAR);
                }
                else {
                    tuRow.addNull();
                }
            }

            if (trip.hasTripId()) {
                if (tuRow == null) {
                    stmt.setString(6, trip.getTripId());
                }
                else {
                    tuRow.add(trip.getTripId());
                }
            }
            else {
                if (tuRow == null) {
                    stmt.setNull(6, Types.VARCHAR);
                }
                else {
                    tuRow.addNull();
                }
            }

            if (trip.hasRouteId()) {
                if (tuRow == null) {
                    stmt.setString(7, trip.getRouteId());
                }
                else {
                    tuRow.add(trip.getRouteId());
                }
            }
            else {
                if (tuRow == null) {
                    stmt.setNull(7, Types.VARCHAR);
                }
                else {
                    tuRow.addNull();
                }
            }
        }
        else {
            if (tuRow == null) {
                stmt.setNull(3, Types.INTEGER);
                stmt.setNull(4, Types.VARCHAR);
                stmt.setNull(5, Types.VARCHAR);
                stmt.setNull(6, Types.VARCHAR);
                stmt.setNull(7, Types.VARCHAR);
            }
            else {
                tuRow.addNull(5);
            }
        }

        if (tripUpdate.hasVehicle()) {
            VehicleDescriptor vd = tripUpdate.getVehicle();

            if (vd.hasId()) {
                if (tuRow == null) {
                    stmt.setString(8, vd.getId());
                }
                else {
                    tuRow.add(vd.getId());
                }
            }
            else {
                if (tuRow == null) {
                    stmt.setNull(8, Types.INTEGER);
                }
                else {
                    tuRow.addNull();
                }
            }

            if (vd.hasLabel()) {
                if (tuRow == null) {
                    stmt.setString(9, vd.getLabel());
                }
                else {
                    tuRow.add(vd.getLabel());
                }
            }
            else {
                if (tuRow == null) {
                    stmt.setNull(9, Types.VARCHAR);
                }
                else {
                    tuRow.addNull();
                }
            }

            if (vd.hasLicensePlate()) {
                if (tuRow == null) {
                    stmt.setString(10, vd.getLicensePlate());
                }
                else {
                    tuRow.add(vd.getLicensePlate());
                }
            }
            else {
                if (tuRow == null) {
                    stmt.setNull(10, Types.VARCHAR);
                }
                else {
                    tuRow.addNull();
                }
            }
        }
        else {
            if (tuRow == null) {
                stmt.setNull(8, Types.INTEGER);
                stmt.setNull(9, Types.VARCHAR);
                stmt.setNull(10, Types.VARCHAR);
            }
            else {
                tuRow.addNull(3);
            }
        }
        
        Date recorded = new Date();
        
        if (tuRow == null) {
            stmt.setInt(11, (int) (recorded.getTime() / 1000));
        }
        else {
            tuRow.add((int) (recorded.getTime() / 1000));
        }

        if (tuCopier == null) {
            stmt.addBatch();
        }
        else {
            tuCopier.add(tuRow);
        }

        if (stCopier == null) {
            stmt = mStatements.get(STTRIPUPDATE_STOPTIMEUPDATES);
        }
        else {
            stmt = null;
        }

        for (StopTimeUpdate stu : tripUpdate.getStopTimeUpdateList()) {
            DataCopierRow stRow = null;

            if (stmt == null) {
                stRow = new DataCopierRow();
            }

            if (stRow == null) {
                stmt.setInt(1, updateId);
            }
            else {
                stRow.add(updateId);
            }

            if (stu.hasArrival()) {
                StopTimeEvent ste = stu.getArrival();

                if (ste.hasTime()) {
                    if (stRow == null) {
                        stmt.setLong(2, ste.getTime());
                    }
                    else {
                        stRow.add(ste.getTime());
                    }
                }
                else {
                    if (stRow == null) {
                        stmt.setNull(2, Types.INTEGER);
                    }
                    else {
                        stRow.addNull();
                    }
                }

                if (ste.hasUncertainty()) {
                    if (stRow == null) {
                        stmt.setInt(3, ste.getUncertainty());
                    }
                    else {
                        stRow.add(ste.getUncertainty());
                    }
                }
                else {
                    if (stRow == null) {
                        stmt.setNull(3, Types.INTEGER);
                    }
                    else {
                        stRow.addNull();
                    }
                }

                if (ste.hasDelay()) {
                    if (stRow == null) {
                        stmt.setInt(4, ste.getDelay());
                    }
                    else {
                        stRow.add(ste.getDelay());
                    }
                }
                else {
                    if (stRow == null) {
                        stmt.setNull(4, Types.INTEGER);
                    }
                    else {
                        stRow.addNull();
                    }
                }
            }
            else {
                if (stRow == null) {
                    stmt.setNull(2, Types.INTEGER);
                    stmt.setNull(3, Types.INTEGER);
                    stmt.setNull(4, Types.INTEGER);
                }
                else {
                    stRow.addNull(3);
                }
            }

            if (stu.hasDeparture()) {
                StopTimeEvent ste = stu.getDeparture();

                if (ste.hasTime()) {
                    if (stRow == null) {
                        stmt.setLong(5, ste.getTime());
                    }
                    else {
                        stRow.add(ste.getTime());
                    }
                }
                else {
                    if (stRow == null) {
                        stmt.setNull(5, Types.INTEGER);
                    }
                    else {
                        stRow.addNull();
                    }
                }

                if (ste.hasUncertainty()) {
                    if (stRow == null) {
                        stmt.setInt(6, ste.getUncertainty());
                    }
                    else {
                        stRow.add(ste.getUncertainty());
                    }
                }
                else {
                    if (stRow == null) {
                        stmt.setNull(6, Types.INTEGER);
                    }
                    else {
                        stRow.addNull();
                    }
                }

                if (ste.hasDelay()) {
                    if (stRow == null) {
                        stmt.setInt(7, ste.getDelay());
                    }
                    else {
                        stRow.add(ste.getDelay());
                    }
                }
                else {
                    if (stRow == null) {
                        stmt.setNull(7, Types.INTEGER);
                    }
                    else {
                        stRow.addNull();
                    }
                }
            }
            else {
                if (stRow == null) {
                    stmt.setNull(5, Types.INTEGER);
                    stmt.setNull(6, Types.INTEGER);
                    stmt.setNull(7, Types.INTEGER);
                }
                else {
                    stRow.addNull(3);
                }
            }

            int srInt = stu.hasScheduleRelationship() ? stu.getScheduleRelationship().getNumber()
                    : com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED_VALUE;
            if (stRow == null) {
                stmt.setInt(8, srInt);
            }
            else {
                stRow.add(srInt);
            }

            if (stu.hasStopId()) {
                if (stRow == null) {
                    stmt.setString(9, stu.getStopId());
                }
                else {
                    stRow.add(stu.getStopId());
                }
            }
            else {
                if (stRow == null) {
                    stmt.setNull(9, Types.VARCHAR);
                }
                else {
                    stRow.addNull();
                }
            }

            int ssInt = stu.hasStopSequence() ? stu.getStopSequence() : -1;

            if (stRow == null) {
                stmt.setInt(10, ssInt);
            }
            else {
                stRow.add(ssInt);
            }

            if (stmt == null) {
                stCopier.add(stRow);
            }
            else {
                stmt.addBatch();
            }
        }
    }

    private void recordAlert(Alert alert) throws SQLException {
        PreparedStatement stmt = mStatements.get(STALERT);

        int updateId = getUpdateId();

        stmt.setInt(1, updateId);

        if (alert.hasHeaderText()) {
            stmt.setString(2, getString(alert.getHeaderText()));
        }
        else {
            stmt.setNull(2, Types.VARCHAR);
        }

        if (alert.hasDescriptionText()) {
            stmt.setString(3, getString(alert.getDescriptionText()));
        }
        else {
            stmt.setNull(3, Types.VARCHAR);
        }

        if (alert.hasCause()) {
            stmt.setInt(4, alert.getCause().getNumber());
        }
        else {
            stmt.setNull(4, Types.INTEGER);
        }

        if (alert.hasEffect()) {
            stmt.setInt(5, alert.getEffect().getNumber());
        }
        else {
            stmt.setNull(5, Types.INTEGER);
        }

        Date recorded = new Date();
        stmt.setInt(6, (int) (recorded.getTime() / 1000));

        stmt.addBatch();

        stmt = mStatements.get(STALERT_TIMERANGES);

        for (TimeRange timeRange : alert.getActivePeriodList()) {
            stmt.setInt(1, updateId);

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

            stmt.addBatch();
        }

        stmt = mStatements.get(STALERT_ENTITIES);

        for (EntitySelector entity : alert.getInformedEntityList()) {
            stmt.clearParameters();

            stmt.setInt(1, updateId);

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

            stmt.addBatch();
        }
    }

    private String getString(TranslatedString str) {
        return str.getTranslation(0).getText();
    }
}
