package com.synclite.consolidator.processor;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;

public class OracleExecutor extends JDBCExecutor {

	public OracleExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
		if (e instanceof java.sql.SQLIntegrityConstraintViolationException) {
			// Check the SQL state and error message to identify the duplicate key violation
			String sqlState = ((java.sql.SQLIntegrityConstraintViolationException) e).getSQLState();
			String errorMessage = e.getMessage();

			if ("23000".equals(sqlState) && errorMessage.contains("unique constraint")) {
				return true;
			}
		}
		return false;	
	}

	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("ORA-00955") && e.getMessage().contains("name is already used by an existing object")) {
			return true;
		}
		return false;
	}

	//For oracle date comes as a timestamp. Try to set a timestamp.
	@Override
	protected void setDate(PreparedStatement pstmt, int i, Object o) throws SQLException {
		super.setTimestamp(pstmt, i, o);
	}
}

