package com.synclite.consolidator.processor;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.synclite.consolidator.connector.JDBCConnector;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.schema.Column;

public class SQLiteExecutor extends JDBCExecutor {
	
	public SQLiteExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}
	
	@Override
	public boolean isDuplicateKeyException(Exception e) {
		//duplicate key value violates unique constraint
		String errorMsg = e.getMessage();
		if (errorMsg.contains("UNIQUE Constraint failed")) {
			return true;
		}
		return false;
	}

	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("Parse error:") && e.getMessage().contains("already exists")) {
			return true;
		}
		return false;
	}

	@Override 
	public String getTelemetryFileURL() {
		return ConfLoader.getInstance().getDstConnStr(dstIndex);
	}

	/*
	@Override
	public void closeTelemetryFile(Path telemetryFilePath) {
		TelemetryFileConnector.resetDataSource("jdbc:sqlite:" + telemetryFilePath);
	}
	*/
	
	@Override
	protected void bindPrepared(PreparedStatement pstmt, int i, Object o, Column c, boolean isConditionalArg) throws SQLException {
		switch(c.type.javaSQLType) {
		case BLOB:
			if (o instanceof InputStream) {
				pstmt.setBinaryStream(i, (InputStream) o);
			} else {
				pstmt.setObject(i, o);
			}
			break;
		case CLOB:
			if (o instanceof InputStream) {
				pstmt.setBinaryStream(i, (InputStream) o);
			} else {
				pstmt.setObject(i, o);
			}
			break;
		default:
			pstmt.setObject(i, o);
		}
	}

	@Override
	protected Connection connectToDst() throws DstExecutionException {
		if (device == null) {
			return JDBCConnector.getInstance(dstIndex).connect();
		}		
		return JDBCConnector.getInstance(dstIndex).connect();    	
	}
	
	@Override
	public void alterColumn(AlterColumn oper) throws DstExecutionException {
		//Skip since SQLite does not support alter column
		this.tracer.info("Skipping Unsupported alter column Oper : " + oper);		
	}
}
