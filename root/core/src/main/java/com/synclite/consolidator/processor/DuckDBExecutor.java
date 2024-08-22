package com.synclite.consolidator.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.synclite.consolidator.connector.JDBCConnector;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.global.ConfLoader;

public class DuckDBExecutor extends FileLoaderExecutor {

	public DuckDBExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
		//enableSingleRecBatchOptimization = false;
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
		//duplicate key value violates unique constraint
		String errorMsg = e.getMessage();
		if (errorMsg.contains("Constraint Error") && errorMsg.contains("violates primary key constraint")) {
			return true;
		}
		return false;
	}

	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("Catalog Error:") && e.getMessage().contains("already exists")) {
			return true;
		}
		return false;
	}

	@Override
	protected void putFile() throws DstExecutionException {
		//Nothing to do here.		
	}

    protected void doAddToPstmtBatch(PreparedStatement pstmt) throws SQLException {
    	//Do nothing as DuckDB does not support
    }

	@Override
    protected void doExcutePstmtBatch(PreparedStatement pstmt) throws SQLException {
		//Just execute the prepared statement
    	pstmt.execute();
    }
	
	@Override 
	public String getTelemetryFileURL() {
		return ConfLoader.getInstance().getDstConnStr(dstIndex);
	}

	/*
	@Override
	public void closeTelemetryFile(Path telemetryFilePath) {
		TelemetryFileConnector.resetDataSource("jdbc:duckdb:" + telemetryFilePath);
	}
	 */
	
	@Override
	protected Connection connectToDst() throws DstExecutionException {
		if (device == null) {
			return JDBCConnector.getInstance(dstIndex).connect();
		}		
		return JDBCConnector.getInstance(dstIndex).connect();    	
	}

}
