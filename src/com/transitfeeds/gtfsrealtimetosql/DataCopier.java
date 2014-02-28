package com.transitfeeds.gtfsrealtimetosql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.copy.CopyIn;

public class DataCopier {

	private List<DataCopierRow> mRows = new ArrayList<DataCopierRow>();
	
	private CopyIn mCopier;
	private String mSeparator;
	
	public DataCopier() {
	}
	
	public DataCopier(CopyIn copier, String separator) {
	    mSeparator = separator;
	    mCopier = copier;
	}
	
	public void add(DataCopierRow row) throws SQLException {
	    if (mCopier == null) {
	        mRows.add(row);
	    }
	    else {
	        row.write(mCopier, mSeparator);
	    }
	}

	public void write(CopyIn copier, String separator) throws SQLException {
	    if (mCopier != null) {
	        return;
	    }
	    
		for (DataCopierRow row : mRows) {
		    row.write(copier, separator);
		}
	}
	
	public int size() {
	    return mRows.size();
	}
}
