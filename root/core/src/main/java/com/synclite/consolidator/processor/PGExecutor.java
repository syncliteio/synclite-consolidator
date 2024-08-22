package com.synclite.consolidator.processor;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;

public class PGExecutor extends JDBCExecutor {

	public PGExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
		//duplicate key value violates unique constraint
		String errorMsg = e.getMessage();
		if (errorMsg.contains("duplicate key value violates unique constraint")) {
			return true;
		}
		return false;
	}
	
	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("relation") && e.getMessage().contains("already exists")) {
			return true;
		}
		return false;
	}

	
	protected void setDate(PreparedStatement pstmt, int i, Object o) throws SQLException {
		try {
			pstmt.setDate(i, Date.valueOf(o.toString()));
		} catch(Exception e) {
			try {
				//Try parsing as a timestamp first.
				//
				Timestamp ts = Timestamp.valueOf(o.toString());
				java.sql.Date sqlDate = new java.sql.Date(ts.getTime());
				pstmt.setDate(i, sqlDate);
			} catch (Exception e1) {
				try {
					pstmt.setString(i, o.toString() + "::date");
				} catch (Exception e2) {
					pstmt.setObject(i, o);
				}
			}
		}
	}


}
