package com.synclite.consolidator.processor;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;

public class ClickHouseExecutor extends JDBCExecutor {

	public ClickHouseExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
		//
		//ClickHouse does not throw duplicate key error
		//
		return false;	
	}
}
