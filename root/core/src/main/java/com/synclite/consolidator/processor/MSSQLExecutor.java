package com.synclite.consolidator.processor;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;

public class MSSQLExecutor extends JDBCExecutor {

	public MSSQLExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
    	String errorMsg = e.getMessage();
		if (errorMsg.contains("Msg 2627")) {
    		if (errorMsg.contains("Violation of PRIMARY KEY constraint")) {
    			return true;
    		}
    	}
		return false;
	}

	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("2714") && e.getMessage().contains("There is already an object named") && e.getMessage().contains("in the database")) {
			return true;
		}
		return false;
	}

}
