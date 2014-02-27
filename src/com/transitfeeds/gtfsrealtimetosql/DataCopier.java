package com.transitfeeds.gtfsrealtimetosql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.copy.CopyIn;

public class DataCopier {

	private List<DataCopierRow> mRows = new ArrayList<DataCopierRow>();
	
	public void add(DataCopierRow row) {
		mRows.add(row);
	}

	public void write(CopyIn copier, String separator) throws SQLException {
		byte[] bytes;
		
		for (DataCopierRow row : mRows) {
			bytes = row.getBytes(separator);
			copier.writeToCopy(bytes, 0, bytes.length);
		}
	}
	
	public int size() {
	    return mRows.size();
	}
}
