package com.synclite.consolidator.processor;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;

public class MySQLExecutor extends JDBCExecutor {

	public MySQLExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
    	String errorMsg = e.getMessage();
    	if (errorMsg.contains("Error Code")) {
    		if (errorMsg.contains("1062")) {
    			if (errorMsg.contains("Duplicate entry")) {
    				return true;
    			}
    		}
    	}
		return false;
	}

	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("1050") && e.getMessage().contains("Table") && e.getMessage().contains("already exists")) {
			return true;
		}
		return false;
	}
}
